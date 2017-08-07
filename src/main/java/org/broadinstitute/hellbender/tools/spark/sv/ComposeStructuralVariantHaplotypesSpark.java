package org.broadinstitute.hellbender.tools.spark.sv;

import htsjdk.samtools.*;
import htsjdk.samtools.util.Locatable;
import htsjdk.samtools.util.SequenceUtil;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextComparator;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.BetaFeature;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.engine.Shard;
import org.broadinstitute.hellbender.engine.ShardBoundary;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.engine.spark.datasources.ReadsSparkSource;
import org.broadinstitute.hellbender.engine.spark.datasources.VariantsSparkSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.bwa.BwaSparkEngine;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.AlignedAssembly.AlignmentInterval;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.AlignedContig;
import org.broadinstitute.hellbender.utils.IntervalUtils;
import org.broadinstitute.hellbender.utils.SATagBuilder;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAligner;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.bwa.BwaMemIndex;
import org.broadinstitute.hellbender.utils.collections.IntervalsSkipList;
import org.broadinstitute.hellbender.utils.gcs.BamBucketIoUtils;
import org.broadinstitute.hellbender.utils.haplotype.Haplotype;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Created by valentin on 7/14/17.
 */
@CommandLineProgramProperties(summary = "test", oneLineSummary = "test",
        programGroup = StructuralVariationSparkProgramGroup.class )
@BetaFeature
public class ComposeStructuralVariantHaplotypesSpark extends GATKSparkTool {

    private static final long serialVersionUID = 1L;

    public static final String CONTIGS_FILE_SHORT_NAME = "C";
    public static final String CONTIGS_FILE_FULL_NAME = "contigs";
    public static final String SHARD_SIZE_SHORT_NAME = "sz";
    public static final String SHARD_SIZE_FULL_NAME = "shardSize";
    public static final String PADDING_SIZE_SHORT_NAME = "pd";
    public static final String PADDING_SIZE_FULL_NAME = "paddingSize";

    public static final int DEFAULT_SHARD_SIZE = 10_000;
    public static final int DEFAULT_PADDING_SIZE = 50;
    public static final int FASTA_BASES_PER_LINE = 60;


    @Argument(doc = "shard size",
              shortName = SHARD_SIZE_SHORT_NAME,
              fullName = SHARD_SIZE_FULL_NAME,
    optional = true)
    private int shardSize = DEFAULT_SHARD_SIZE;

    @Argument(doc ="padding size",
              shortName = PADDING_SIZE_SHORT_NAME,
              fullName = PADDING_SIZE_FULL_NAME,
              optional = true)
    private int paddingSize = DEFAULT_PADDING_SIZE;

    @Argument(doc = "input variant file",
              shortName = StandardArgumentDefinitions.VARIANT_SHORT_NAME,
              fullName = StandardArgumentDefinitions.VARIANT_LONG_NAME)
    private String variantsFileName;

    @Argument(doc = "aligned contig file",
              fullName = CONTIGS_FILE_FULL_NAME,
              shortName = CONTIGS_FILE_SHORT_NAME
    )
    private String alignedContigsFileName;

    @Argument(doc = "output bam file with contigs per variant",
              fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
              shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME
    )
    private String outputFileName;

    @Override
    protected void onStartup() {
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        Utils.nonNull(ctx);
        Utils.nonNull(alignedContigsFileName);
        final ReadsSparkSource alignedContigs = new ReadsSparkSource(ctx);
        final VariantsSparkSource variantsSource = new VariantsSparkSource(ctx);

        final JavaRDD<GATKRead> contigs = alignedContigs.getParallelReads(alignedContigsFileName, referenceArguments.getReferenceFileName(), getIntervals());
        final JavaRDD<VariantContext> variants = variantsSource.getParallelVariantContexts(variantsFileName, getIntervals()).filter(ComposeStructuralVariantHaplotypesSpark::supportedVariant);

        final JavaPairRDD<VariantContext, List<GATKRead>> variantOverlappingContigs = composeOverlappingContigs(ctx, contigs, variants);
        processVariants(ctx, variantOverlappingContigs, getReferenceSequenceDictionary(), alignedContigs);

    }

    private static boolean supportedVariant(final VariantContext vc) {
        final List<Allele> alternatives = vc.getAlternateAlleles();
        if (alternatives.size() != 1) {
            return false;
        } else {
            final Allele alternative = alternatives.get(0);
            return StructuralVariantAllele.isStructural(alternative) &&
                    (StructuralVariantAllele.valueOf(alternative) == StructuralVariantAllele.INS
                            || StructuralVariantAllele.valueOf(alternative) == StructuralVariantAllele.DEL);
        }
    }

    private JavaPairRDD<VariantContext,List<GATKRead>> composeOverlappingContigs(final JavaSparkContext ctx, final JavaRDD<GATKRead> contigs, final JavaRDD<VariantContext> variants) {
        final SAMSequenceDictionary sequenceDictionary = getBestAvailableSequenceDictionary();
        final List<SimpleInterval> intervals = hasIntervals() ? getIntervals() : IntervalUtils.getAllIntervalsForReference(sequenceDictionary);
        // use unpadded shards (padding is only needed for reference bases)
        final List<ShardBoundary> shardBoundaries = intervals.stream()
                .flatMap(interval -> Shard.divideIntervalIntoShards(interval, shardSize, 0, sequenceDictionary).stream())
                .collect(Collectors.toList());
        final IntervalsSkipList<SimpleInterval> shardIntervals = new IntervalsSkipList<>(shardBoundaries.stream()
                .map(ShardBoundary::getPaddedInterval)
                .collect(Collectors.toList()));
        final Broadcast<SAMSequenceDictionary> dictionaryBroadcast = ctx.broadcast(sequenceDictionary);

        final Broadcast<IntervalsSkipList<SimpleInterval>> shardIntervalsBroadcast = ctx.broadcast(shardIntervals);

        final JavaPairRDD<SimpleInterval, List<Tuple2<SimpleInterval,GATKRead>>> contigsInShards =
            groupInShards(contigs, ComposeStructuralVariantHaplotypesSpark::contigIntervals, shardIntervalsBroadcast);
        final int paddingSize = this.paddingSize;

        final JavaPairRDD<SimpleInterval, List<Tuple2<SimpleInterval, VariantContext>>> variantsInShards =
            groupInShards(variants, (v) -> variantsBreakPointIntervals(v, paddingSize, dictionaryBroadcast.getValue()), shardIntervalsBroadcast);

        final JavaPairRDD<SimpleInterval, Tuple2<List<Tuple2<SimpleInterval, GATKRead>>, List<Tuple2<SimpleInterval, VariantContext>>>> contigAndVariantsInShards =
                contigsInShards.join(variantsInShards);


        final JavaPairRDD<VariantContext, List<GATKRead>> contigsPerVariantInterval =
                contigAndVariantsInShards.flatMapToPair(t -> {
                    final List<Tuple2<SimpleInterval, VariantContext>> vars = t._2()._2();
                    final List<Tuple2<SimpleInterval, GATKRead>> ctgs = t._2()._1();
                    return vars.stream()
                            .map(v -> {
                                final List<GATKRead> cs = ctgs.stream()
                                        .filter(ctg -> v._1().overlaps(ctg._1()))
                                        .map(Tuple2::_2)
                                        .collect(Collectors.toList());

                                return new Tuple2<>(v._2(), cs);})
                            .collect(Collectors.toList()).iterator();
                });

        final Function<VariantContext, String> variantId = (Function<VariantContext, String> & Serializable) ComposeStructuralVariantHaplotypesSpark::variantId;
        final Function2<List<GATKRead>, List<GATKRead>, List<GATKRead>> readListMerger = (a, b) -> Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());

        // Merge contig lists on the same variant-context coming from different intervals
        // into one.
        final JavaPairRDD<VariantContext, List<GATKRead>> contigsPerVariant = contigsPerVariantInterval
                .mapToPair(t -> new Tuple2<>(variantId.apply(t._1()), t))
                .reduceByKey((a, b) -> new Tuple2<>(a._1(), readListMerger.call(a._2(), b._2())))
                .mapToPair(Tuple2::_2);
        return contigsPerVariant;
    }

    private static String variantId(final VariantContext variant) {
        if (variant.getID() != null && !variant.getID().isEmpty()) {
            return variant.getID();
        } else {
            final int length = Math.abs(variantLength(variant));
            return "var_" + variant.getAlternateAllele(0).getDisplayString() + "_" + length;
        }
    }

    private static List<SimpleInterval> contigIntervals(final GATKRead contig) {
        if (contig.isUnmapped()) {
            return Collections.emptyList();
        } else {
            return Collections.singletonList(new SimpleInterval(contig.getContig(), contig.getStart(), contig.getEnd()));
        }
    }

    private static List<SimpleInterval> variantsBreakPointIntervals(final VariantContext variant, final int padding, final SAMSequenceDictionary dictionary) {
        final String contigName = variant.getContig();
        final int contigLength = dictionary.getSequence(contigName).getSequenceLength();
        if (variant.getAlternateAllele(0).getDisplayString().equals("<INS>")) {
            return Collections.singletonList(new SimpleInterval(contigName, Math.max(1, variant.getStart() - padding), Math.min(variant.getStart() + 1 + padding, contigLength)));
        } else { // must be <DEL>
            final int length = - variantLength(variant);
            return Arrays.asList(new SimpleInterval(contigName, Math.max(1, variant.getStart() - padding) , Math.min(contigLength, variant.getStart() + padding)),
                                 new SimpleInterval(contigName, Math.max(1, variant.getStart() + length - padding), Math.min(contigLength, variant.getStart() + length + padding)));
        }
    }

    private static int variantLength(VariantContext variant) {
        final int length = variant.getAttributeAsInt("SVLEN", 0);
        if (length == 0) {
            throw new IllegalStateException("missing SVLEN annotation in " + variant.getContig() + ":" + variant.getStart());
        }
        return length;
    }

    private static SimpleInterval locatableToSimpleInterval(final Locatable loc) {
        return new SimpleInterval(loc.getContig(), loc.getStart(), loc.getEnd());
    }

    private static JavaRDD<GATKRead> readsByInterval(final SimpleInterval interval, final ReadsSparkSource alignedContigs, final String alignedContigFileName, final String referenceName) {

        return alignedContigs.getParallelReads(alignedContigFileName,
                referenceName, Collections.singletonList(interval));
    }

    private <T> JavaPairRDD<SimpleInterval, List<Tuple2<SimpleInterval, T>>> groupInShards(final JavaRDD<T> elements, final org.apache.spark.api.java.function.Function<T, List<SimpleInterval>> intervalsOf,
                                                                  final Broadcast<IntervalsSkipList<SimpleInterval>> shards) {
        final PairFlatMapFunction<T, SimpleInterval, Tuple2<SimpleInterval, T>> flatMapIntervals =
                t -> intervalsOf.call(t).stream().map(i -> new Tuple2<>(i, new Tuple2<>(i,t))).iterator();

        JavaPairRDD<Integer, Integer> x;

        return elements
                .flatMapToPair(flatMapIntervals)
                .flatMapToPair(t -> shards.getValue().getOverlapping(t._1()).stream().map(i -> new Tuple2<>(i, t._2())).iterator())
                .aggregateByKey(new ArrayList<Tuple2<SimpleInterval, T>>(10),
                        (l1, c) -> { l1.add(c); return l1;},
                        (l1, l2) -> {l1.addAll(l2); return l1;});
    }

    protected void processVariants(final JavaSparkContext ctx, final JavaPairRDD<VariantContext, List<GATKRead>> variantsAndOverlappingContigs, final SAMSequenceDictionary dictionary, final ReadsSparkSource s) {

        final SAMFileHeader outputHeader = new SAMFileHeader();
        final SAMProgramRecord programRecord = new SAMProgramRecord(getProgramName());
        programRecord.setCommandLine(getCommandLine());
        outputHeader.setSequenceDictionary(dictionary);
        outputHeader.addProgramRecord(programRecord);
        outputHeader.setSortOrder(SAMFileHeader.SortOrder.coordinate);
        outputHeader.addReadGroup(new SAMReadGroupRecord("CTG"));
        final SAMFileWriter outputWriter = BamBucketIoUtils.makeWriter(outputFileName, outputHeader, false);


        final BinaryOperator<GATKRead> readMerger = (BinaryOperator<GATKRead> & Serializable)
                ComposeStructuralVariantHaplotypesSpark::reduceContigReads;

        final VariantContextComparator variantComparator = new VariantContextComparator(dictionary);

        final JavaPairRDD<VariantContext, List<GATKRead>> variantsAndOverlappingUniqueContigs
                = variantsAndOverlappingContigs
                .mapValues(l -> l.stream().collect(Collectors.groupingBy(GATKRead::getName)))
                .mapValues(m -> m.values().stream()
                        .map(l -> l.stream().reduce(readMerger).orElseThrow(IllegalStateException::new))
                        .collect(Collectors.toList()));

        Utils.stream(variantsAndOverlappingUniqueContigs.toLocalIterator())
                .map(t -> resolvePendingContigs(t, s))
                .forEach(t -> {
                    final StructuralVariantContext svc = new StructuralVariantContext(t._1());
                    final List<GATKRead> contigs = t._2();
                    final int maxLength = contigs.stream()
                            .mapToInt(GATKRead::getLength)
                            .max().orElse(paddingSize);
                    final Haplotype referenceHaplotype = svc.composeHaplotype(0, maxLength * 2, getReference());
                    //referenceHaplotype.setGenomeLocation(null);
                    final Haplotype alternativeHaplotype = svc.composeHaplotype(1, maxLength * 2, getReference());
                    //alternativeHaplotype.setGenomeLocation(null);
                    final String idPrefix = String.format("var_%s_%d", t._1().getContig(), t._1().getStart());
                    outputHaplotypesAsSAMRecords(outputHeader, outputWriter, referenceHaplotype, alternativeHaplotype, idPrefix);
                    final Map<String, AlignedContig> referenceAlignedContigs = alignContigsAgainstHaplotype(ctx, outputHeader, referenceHaplotype, contigs);
                    final Map<String, AlignedContig> alternativeAlignedContigs = alignContigsAgainstHaplotype(ctx, outputHeader, alternativeHaplotype, contigs);
                    contigs.forEach(contig -> {
                        final AlignedContig referenceAlignment = referenceAlignedContigs.get(contig.getName());
                        final AlignedContig alternativeAlignment = alternativeAlignedContigs.get(contig.getName());
                        final AlignedContigScore referenceScore = calculateAlignedContigScore(referenceAlignment);
                        final AlignedContigScore alternativeScore = calculateAlignedContigScore(alternativeAlignment);
                        final String hpTagValue = calculateHPTag(referenceScore.getValue(), alternativeScore.getValue());
                        final double hpQualTagValue = calculateHPQualTag(referenceScore.getValue(), alternativeScore.getValue());
                        if (!referenceAlignment.alignmentIntervals.isEmpty()) {
                            final List<SAMRecord> records = convertToSAMRecords(referenceAlignment, outputHeader, t._1().getContig(), referenceHaplotype.getGenomeLocation().getStart(), idPrefix, hpTagValue, hpQualTagValue, referenceScore, alternativeScore, t._1().getStart());
                            records.forEach(outputWriter::addAlignment);
                        } else {
                            final SAMRecord outputRecord = convertToUnmappedSAMRecord(outputHeader, t._1().getContig(), t._1().getStart(), idPrefix, contig, referenceScore, alternativeScore, hpTagValue, hpQualTagValue);
                            outputWriter.addAlignment(outputRecord);
                        }
                    });
                });
        outputWriter.close();
    }

    private SAMRecord convertToUnmappedSAMRecord(SAMFileHeader outputHeader, final String referenceContig
            , final int start, String idPrefix, GATKRead originalContig, AlignedContigScore referenceScore, AlignedContigScore alternativeScore, String hpTagValue, double hpQualTagValue) {
        final SAMRecord outputRecord = new SAMRecord(outputHeader);
        outputRecord.setAttribute(SAMTag.RG.name(), "CTG");
        outputRecord.setAttribute("HP", hpTagValue);
        outputRecord.setAttribute("HQ", "" + hpQualTagValue);
        outputRecord.setAttribute("RS", "" + referenceScore);
        outputRecord.setAttribute("XS", "" + alternativeScore);
        outputRecord.setAttribute("VC", "" + referenceContig + ":" + start);
        outputRecord.setReadName(idPrefix + ":" + originalContig.getName());
        outputRecord.setReadPairedFlag(false);
        outputRecord.setDuplicateReadFlag(false);
        outputRecord.setSecondOfPairFlag(false);
        outputRecord.setCigarString("*");
        outputRecord.setReadNegativeStrandFlag(false);
        final byte[] bases = originalContig.getBases();
        final byte[] quals = originalContig.getBaseQualities();
        if (originalContig.isReverseStrand()) {
            SequenceUtil.reverseComplement(bases);
            if (quals != null && quals.length > 0) {
                SequenceUtil.reverseQualities(quals);
            }
        }
        outputRecord.setReadBases(bases);
        outputRecord.setBaseQualities(quals);
        outputRecord.setReferenceName(referenceContig);
        outputRecord.setAlignmentStart(start);
        outputRecord.setMappingQuality(0);
        outputRecord.setReadUnmappedFlag(true);
        return outputRecord;
    }

    private List<SAMRecord> convertToSAMRecords(final AlignedContig alignment, final SAMFileHeader header, final String referenceContig, final int contigStart, final String idPrefix, final String hpTagValue, final double hpQualTagValue, final AlignedContigScore referenceScore, final AlignedContigScore alternativeScore, final int variantStart) {
        final List<SAMRecord> result = new ArrayList<>(alignment.alignmentIntervals.size());
        result.add(alignment.alignmentIntervals.get(0).convertToSAMRecord(header, alignment, false));
        for (int i = 1; i < alignment.alignmentIntervals.size(); i++) {
            result.add(alignment.alignmentIntervals.get(i).convertToSAMRecord(header, alignment, true));
        }
        for (final SAMRecord record : result) {
            record.setReadName(idPrefix + ":" + alignment.contigName);
            record.setReferenceName(referenceContig);
            record.setAlignmentStart(record.getAlignmentStart() + contigStart - 1);
            record.setAttribute(SAMTag.RG.name(), "CTG");
            record.setAttribute("HP", hpTagValue);
            record.setAttribute("HQ", "" + hpQualTagValue);
            record.setAttribute("RS", "" + referenceScore);
            record.setAttribute("XS", "" + alternativeScore);
            record.setAttribute("VC", "" + referenceContig + ":" + variantStart);
        }
        final List<String> saTagValues = result.stream()
                .map(record ->
                    Utils.join(",", record.getReferenceName(), record.getAlignmentStart(), record.getReadNegativeStrandFlag() ? "+" : "-", record.getCigarString(), record.getMappingQuality(), "" + record.getAttribute(SAMTag.NM.name()))
                )
                .collect(Collectors.toList());
        if (result.size() > 1) {
            for (int i = 0; i < result.size(); i++) {
                final int idx = i;
                result.get(i).setAttribute(SAMTag.SA.name(),
                        IntStream.range(0, result.size())
                        .filter(ii -> ii != idx)
                        .mapToObj(saTagValues::get)
                        .collect(Collectors.joining(";")) + ";");
            }
        }
        return result;
    }


    private static class AlignedContigScore {
        final int totalReversals;
        final int totalIndels;
        final int totalMatches;
        final int totalMismatches;
        final int totalIndelLength;

        public AlignedContigScore(final int reversals, final int indels, final int matches, final int mismatches, final int totalIndelLength) {
            this.totalReversals = reversals;
            this.totalIndels = indels;
            this.totalMatches = matches;
            this.totalMismatches = mismatches;
            this.totalIndelLength = totalIndelLength;
        }

        public double getValue() {
            return -(int) Math.round(totalMatches * 0.01  + totalMismatches * 30 + totalIndels * 45 + (totalIndelLength - totalIndels) * 3
                    + totalReversals * 60);
        }

        public String toString() {
            return  getValue() + ":" + Utils.join(",", totalMatches, totalMismatches, totalIndels, totalIndelLength, totalReversals);
        }
    }

    private AlignedContigScore calculateAlignedContigScore(final AlignedContig ctg) {
        final List<AlignmentInterval> intervals = ctg.alignmentIntervals.stream()
                .sorted(Comparator.comparing(ai -> ai.startInAssembledContig))
                .collect(Collectors.toList());
        int totalReversals = 0;
        int totalIndels = 0;
        int totalMatches = 0;
        int totalMismatches = 0;
        int totalIndelLength = 0;
        for (int i = 0; i < intervals.size(); i++) {
            final AlignmentInterval ai = ctg.alignmentIntervals.get(i);
            if (i > 0) {
                if (intervals.get(i - 1).forwardStrand != ai.forwardStrand) {
                    totalReversals++;
                } else {
                    final AlignmentInterval left = ai.forwardStrand ? intervals.get(i - 1) : ai;
                    final AlignmentInterval right = ai.forwardStrand ? ai : intervals.get(i - 1);
                    if (left.referenceInterval.getEnd() < right.referenceInterval.getStart()) {
                        totalIndels++;
                        totalIndelLength += ai.referenceInterval.getStart() - intervals.get(i - 1).referenceInterval.getEnd();
                    }
                    if (left.endInAssembledContig < right.startInAssembledContig) {
                        totalIndels++;
                        totalIndelLength += right.startInAssembledContig - left.endInAssembledContig - 1;
                    }
                }
            }
            final int matches = ai.cigarAlong5to3DirectionOfContig.getCigarElements().stream()
                    .filter(ce -> ce.getOperator().isAlignment())
                    .mapToInt(CigarElement::getLength).sum();
            final int misMatches = ai.mismatches;
            final int indelCount = (int) ai.cigarAlong5to3DirectionOfContig.getCigarElements().stream()
                    .filter(ce -> ce.getOperator().isIndel())
                    .count();
            final int indelLengthSum = ai.cigarAlong5to3DirectionOfContig.getCigarElements().stream()
                    .filter(ce -> ce.getOperator().isIndel())
                    .mapToInt(CigarElement::getLength).sum();
            totalIndels += indelCount;
            totalMatches += matches;
            totalMismatches += misMatches;
            totalIndelLength += indelLengthSum;
        }
        if (intervals.isEmpty()) {
            totalIndelLength += ctg.contigSequence.length;
            totalIndels++;
        } else {
            if (intervals.get(0).startInAssembledContig > 1) {
                totalIndelLength += intervals.get(0).startInAssembledContig - 1;
                totalIndels++;
            }
            if (intervals.get(intervals.size() - 1).endInAssembledContig < ctg.contigSequence.length) {
                totalIndelLength += ctg.contigSequence.length - intervals.get(intervals.size() - 1).endInAssembledContig;
                totalIndels++;
            }
        }
        return new AlignedContigScore(totalReversals, totalIndels, totalMatches, totalMismatches, totalIndelLength);
    }

    private String calculateHPTag(final double referenceScore, final double alternativeScore) {
        if (Double.isNaN(referenceScore) && Double.isNaN(alternativeScore)) {
            return ".";
        } else if (Double.isNaN(referenceScore)) {
            return "alt";
        } else if (Double.isNaN(alternativeScore)) {
            return "ref";
        } else {
            switch (Double.compare(alternativeScore, referenceScore)) {
                case 0: return ".";
                case -1: return "ref";
                default: return "alt";
            }
        }
    }

    private double calculateHPQualTag(final double referenceScore, final double alternativeScore) {
        if (Double.isNaN(referenceScore) || Double.isNaN(alternativeScore)) {
            return Double.NaN;
        } else {
            switch (Double.compare(alternativeScore, referenceScore)) {
                case 0: return 0;
                case -1: return referenceScore - alternativeScore;
                default: return alternativeScore - referenceScore;
            }
        }
    }

    private final Map<String,AlignedContig> alignContigsAgainstHaplotype(final JavaSparkContext ctx, final SAMFileHeader header, final Haplotype haplotype, final List<GATKRead> contigs) {
        File fastaFile = null;
        File imageFile = null;
        try {
            fastaFile = createFastaFromHaplotype(haplotype);
            imageFile = createImageFileFromFasta(fastaFile);
            final Stream<Tuple2<GATKRead, List<AlignmentInterval>>> alignedContigSegments;
            if (contigs.size() > 10) { // just bother to sparkify it if there is a decent number of contigs.
                final Map<String, GATKRead> contigsByName = contigs.stream()
                        .collect(Collectors.toMap(GATKRead::getName, Function.identity()));
                final BwaSparkEngine bwa = new BwaSparkEngine(ctx, imageFile.getPath(), header, header.getSequenceDictionary());
                alignedContigSegments = bwa.alignSingletons(ctx.parallelize(contigs))
                        .mapToPair(r -> new Tuple2<>(r.getName(), r))
                        .mapValues(r -> new AlignmentInterval(r))
                        .combineByKey(
                                ai -> { final List<AlignmentInterval> result = new ArrayList<>();
                                        result.add(ai);
                                        return result; },
                                (l, ai) -> { l.add(ai); return l; },
                                (l, k) -> { l.addAll(k); return l;} )
                        .collectAsMap().entrySet().stream()
                        .map(entry -> new Tuple2<>(contigsByName.get(entry.getKey()), entry.getValue()));
                bwa.close();
            } else {
                final BwaMemIndex index = new BwaMemIndex(imageFile.getPath());
                final BwaMemAligner aligner = new BwaMemAligner(index);
                final List<List<BwaMemAlignment>> alignments = aligner.alignSeqs(contigs, GATKRead::getBases);
                alignedContigSegments = IntStream.range(0, contigs.size())
                        .mapToObj(i -> new Tuple2<>(contigs.get(i), alignments.get(i)))
                        .map(t -> new Tuple2<>(t._1(), t._2().stream()
                                .filter(bwa -> bwa.getRefId() >= 0) // remove unmapped reads.
                                .map(bma -> new AlignmentInterval(Utils.nonNull(bma), header.getSequenceDictionary(), t._1().getLength()))
                                .collect(Collectors.toList())));
            }
            return alignedContigSegments
                    .map(t -> new AlignedContig(t._1().getName(), t._1().getBases(), t._2()))
                    .map(a -> {
                        final Tuple2<List<AlignmentInterval>, Double> bestConfiguration =
                                PlaygroundExtract.pickAnyBestConfiguration(a, Collections.singleton("seq"));
                        return new AlignedContig(a.contigName, a.contigSequence, bestConfiguration._1(), bestConfiguration._2());
                    })
                    .collect(Collectors.toMap(a -> a.contigName, a -> a));
        } finally {
            Stream.of(fastaFile, imageFile)
                    .filter(Objects::nonNull)
                    .forEach(File::delete);
        }
    }

    private File createFastaFromHaplotype(final Haplotype haplotype) {
        final File result;
        try {
            result = File.createTempFile("gatk-sv-bwa-tmp-ref-", ".fasta");
        } catch (final IOException ex) {
            throw new GATKException("cound not optain a file location for a temporary haplotype reference fasta file", ex);
        }
        result.deleteOnExit();
        try (final PrintWriter fastaWriter = new PrintWriter(new FileWriter(result))) {

            fastaWriter.println(">seq");
            final byte[] bases = haplotype.getBases();
            int nextIdx = 0;
            while (nextIdx < bases.length) {
                fastaWriter.println(new String(bases, nextIdx, Math.min(bases.length - nextIdx, FASTA_BASES_PER_LINE)));
                nextIdx += FASTA_BASES_PER_LINE;
            }
        } catch (final IOException ex) {
            throw new GATKException("could not write haplotype reference fasta file '" + result + "'", ex);
        }
        return result;
    }

    private File createImageFileFromFasta(final File fasta) {
        final File result = new File(fasta.getPath().replaceAll("\\.*$",".img"));
        try {
            BwaMemIndex.createIndexImageFromFastaFile(fasta.getPath(), result.getPath());
        } catch (final Throwable ex) {
            throw new GATKException("problems indexing fasta file '" + fasta + "' into '" + result + "'", ex);
        }
        result.deleteOnExit();
        return result;
    }

    private void outputHaplotypesAsSAMRecords(SAMFileHeader outputHeader, SAMFileWriter outputWriter, Haplotype referenceHaplotype, Haplotype alternativeHaplotype, String idPrefix) {
        final Consumer<SAMRecord> haplotypeExtraSetup = r -> {
            //r.setReferenceName(t._1().getContig());
            //
            // r.setAlignmentStart(t._1().getStart());
            r.setAttribute(SAMTag.RG.name(), "HAP");
            r.setMappingQuality(60);
        };
        outputWriter.addAlignment(referenceHaplotype.convertToSAMRecord(outputHeader, idPrefix + ":ref",
                haplotypeExtraSetup));
        outputWriter.addAlignment(alternativeHaplotype.convertToSAMRecord(outputHeader, idPrefix + ":alt",
                haplotypeExtraSetup));
    }

    private Tuple2<VariantContext, List<GATKRead>> resolvePendingContigs(final Tuple2<VariantContext, List<GATKRead>> vc, final ReadsSparkSource s) {
        logger.debug("VC " + vc._1().getContig() + ":" + vc._1().getStart() + "-" + vc._1().getEnd());
        final List<GATKRead> updatedList = vc._2().stream()
                .map(r -> {
                    if (!r.getCigar().containsOperator(CigarOperator.H)) {
                        return r;
                    } else {
                        final SATagBuilder saTagBuilder = new SATagBuilder(r);
                        final String targetName = r.getName();
                        final List<SimpleInterval> locations = saTagBuilder.getArtificialReadsBasedOnSATag(s.getHeader(alignedContigsFileName, referenceArguments.getReferenceFileName()))
                                .stream().map(rr -> new SimpleInterval(rr.getContig(), rr.getStart(), rr.getEnd()))
                                .collect(Collectors.toList());
                        final GATKRead candidate = s.getParallelReads(alignedContigsFileName, referenceArguments.getReferenceFileName(), locations)
                                .filter(rr -> rr.getName().equals(targetName))
                                .reduce(((Function2< GATKRead, GATKRead, GATKRead> & Serializable) ComposeStructuralVariantHaplotypesSpark::reduceContigReads));

                        final GATKRead canonicRead = reduceContigReads(candidate, r);
                        if (canonicRead.getCigar().containsOperator(CigarOperator.H)) {
                            logger.warn("Contig " + canonicRead.getName() + " " + readAlignmentString(canonicRead) + " " + canonicRead.getAttributeAsString("SA") + " gave-up!");
                            return null;
                        } else {
                            return canonicRead;
                        }
                    }})
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return new Tuple2<>(vc._1, updatedList);
    }

    private static String readAlignmentString(GATKRead r) {
        return r.getContig() + "," + r.getStart() + "," + (r.isReverseStrand() ?  "-" : "+") + "," + r.getCigar() + "," + r.getMappingQuality() + "," + r.getAttributeAsString("NM");
    }

    private static GATKRead reduceContigReads(final GATKRead read1, final GATKRead read2) {
        if (read2.isUnmapped() || read2.isSecondaryAlignment()) {
            return read1;
        } else if (read1.isUnmapped() || read1.isSecondaryAlignment()) {
            return read2;
        } else if (!containsHardclips(read1)) {
            return read1;
        } else if (!containsHardclips(read2)) {
            return read2;
        } else {
            return mergeSequences(read1, read2);
        }
    }

    private static boolean containsHardclips(final GATKRead read1) {
        return read1.getCigar().getCigarElements().stream().anyMatch(e -> e.getOperator() == CigarOperator.HARD_CLIP);
    }

    private static GATKRead mergeSequences(final GATKRead read1, final GATKRead read2) {
        final int contigLength = contigLength(read1);
        final byte[] bases = new byte[contigLength];
        final MutableInt start = new MutableInt(contigLength);
        final MutableInt end = new MutableInt(-1);
        final SATagBuilder saBuilder1 = new SATagBuilder(read1);
        final SATagBuilder saBuilder2 = new SATagBuilder(read2);
        saBuilder1.removeTag(read2);
        saBuilder1.removeTag(read1);
        mergeSequences(bases, start, end, read1);
        mergeSequences(bases, start, end, read2);
        final byte[] mergedBases = Arrays.copyOfRange(bases, start.intValue(), end.intValue());
        final List<CigarElement> elements = new ArrayList<>();
        if (start.intValue() > 0) {
            elements.add(new CigarElement(start.intValue(), CigarOperator.H));
        }
        elements.add(new CigarElement(end.intValue() - start.intValue(), CigarOperator.M));
        if (end.intValue() < contigLength) {
            elements.add(new CigarElement(contigLength - end.intValue(), CigarOperator.H));
        }
        final Cigar mergedCigar = new Cigar(elements);

        final GATKRead result = new ContigMergeGATKRead(read1.getName(), read1.getContig(), read1.getStart(), mergedBases,
                mergedCigar, Math.max(read1.getMappingQuality(), read2.getMappingQuality()), bases.length, read1.getReadGroup(), read1.isSupplementaryAlignment());

        final SATagBuilder resultSABuilder = new SATagBuilder(result);

        resultSABuilder.addAllTags(saBuilder1);
        resultSABuilder.addAllTags(saBuilder2);
        resultSABuilder.setSATag();
        return result;
    }

    private static void mergeSequences(final byte[] bases, final MutableInt start, final MutableInt end, final GATKRead read) {
        final byte[] readBases = read.getBases();
        Cigar cigar = read.getCigar();
        if (read.isReverseStrand()) {
            SequenceUtil.reverseComplement(readBases, 0, readBases.length);
            cigar = CigarUtils.invertCigar(cigar);
        }
        int nextReadBase = 0;
        int nextBase = 0;
        int hardClipStart = 0; // unless any leading H found.
        int hardClipEnd = bases.length; // unless any tailing H found.
        for (final CigarElement element : cigar) {
            final CigarOperator operator = element.getOperator();
            final int length = element.getLength();
            if (operator == CigarOperator.H) {
                    if (nextBase == 0) { // hard-clip at the beginning.
                        hardClipStart = length;
                    } else { // hard-clip at the end.
                        hardClipEnd = bases.length - length;
                    }
                nextBase += length;
            } else if (operator.consumesReadBases()) {
                for (int i = 0; i < length; i++) {
                    bases[nextBase + i] = mergeBase(bases[nextBase + i], readBases[nextReadBase + i], () -> "mismatching bases");
                }
                nextBase += element.getLength();
                nextReadBase += element.getLength();
            }
        }
        if (hardClipStart < start.intValue()) {
            start.setValue(hardClipStart);
        }
        if (hardClipEnd > end.intValue()) {
            end.setValue(hardClipEnd);
        }
    }

    private static byte mergeQual(final byte a, final byte b) {
        return (byte) Math.max(a, b);
    }

    private static byte mergeBase(final byte a, final byte b, final Supplier<String> errorMessage) {
        if (a == 0) {
            return b;
        } else if (b == 0) {
            return a;
        } else if (a == b) {
            return a;
        } else {
            throw new IllegalStateException(errorMessage.get());
        }
    }


    private static int contigLength(final GATKRead contig) {
        return contig.getCigar().getCigarElements().stream()
                .filter(e -> e.getOperator() == CigarOperator.H || e.getOperator().consumesReadBases())
                .mapToInt(CigarElement::getLength)
                .sum();
    }
}

