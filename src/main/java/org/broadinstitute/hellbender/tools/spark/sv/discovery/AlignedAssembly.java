package org.broadinstitute.hellbender.tools.spark.sv.discovery;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.*;
import htsjdk.samtools.util.CigarUtil;
import htsjdk.samtools.util.SequenceUtil;
import htsjdk.tribble.annotation.Strand;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.SVConstants;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVFastqUtils;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAligner;
import org.broadinstitute.hellbender.utils.bwa.BwaMemAlignment;
import org.broadinstitute.hellbender.utils.read.CigarUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.*;
import java.util.function.IntFunction;

/**
 * Holding necessary information about a local assembly for use in SV discovery.
 */
@DefaultSerializer(AlignedAssembly.Serializer.class)
public final class AlignedAssembly {

    public final int assemblyId;

    public final List<AlignedContig> alignedContigs;

    /**
     * Each assembled contig should have at least one such accompanying structure, or 0 when it is unmapped.
     */
    @DefaultSerializer(AlignmentInterval.Serializer.class)
    static public final class AlignmentInterval {

        public static final int MISSING_AS = -1;
        public final SimpleInterval referenceInterval;
        public final int startInAssembledContig;   // 1-based, inclusive
        public final int endInAssembledContig;     // 1-based, inclusive

        public final Cigar cigarAlong5to3DirectionOfContig;

        public final boolean forwardStrand;
        public final int mapQual;
        public final int mismatches;
        public final int alignmentScore;

        /**
         * Compose an alignment interval instance from a SAM supplementary alignment formatted string.
         * <p>
         *     The input string format is:
         *     <pre>
         *         chr-name,start,strand,cigar,mq,nm,as
         *     </pre>
         *     where mq (mapping-quality), nm (number of mismatches) and as (alignment score) might be absent. The strand
         *     is symbolized as '+' for the forward-strand and '-' for the reverse-strand.
         *     <p>Examples:</p>
         *     <pre>
         *         chr10,1241241,+,10S1313M45I14M100H,30,5,66
         *         chr10,1241241,+,10S1313M45I14M100H,30,5
         *         chr10,1241241,+,10S1313M45I14M100H,30
         *         chr10,1241241,+,10S1313M45I14M100H
         *     </pre>
         * </p>
         * @param str the input string.
         * @throws IllegalArgumentException if {@code str} is {@code null} or it does not look like a valid
         *   SA string.
         */
        public AlignmentInterval(final String str) {
            Utils.nonNull(str, "input str cannot be null");
            final String[] parts = str.replaceAll(";$", "").split(",");
            if (parts.length < 4) {
                throw new IllegalArgumentException("the input SA string at least must contain 4 parts");
            }
            final String referenceContig = parts[0];
            final int start = Integer.parseInt(parts[1]);
            final Strand strand = Strand.toStrand(parts[2]);
            if (strand == Strand.NONE) {
                throw new IllegalArgumentException("the input strand cannot be " + Strand.NONE);
            }
            final boolean forwardStrand = strand == Strand.POSITIVE;
            final Cigar originalCigar;
            try {
                originalCigar = TextCigarCodec.decode(parts[3]);
            } catch (final RuntimeException ex) {
                throw new IllegalArgumentException("bad formatted cigar " + parts[3]);
            }
            final Cigar cigar = forwardStrand ? originalCigar : CigarUtils.invertCigar(originalCigar);
            final int mappingQuality = parts.length >= 5 ? Integer.parseInt(parts[4]) : 0;
            final int mismatches = parts.length >= 6 ? Integer.parseInt(parts[5]) : -1;
            final int alignmentScore = parts.length >= 7 ? Integer.parseInt(parts[6]) : -1;
            this.referenceInterval = new SimpleInterval(referenceContig, start,
                    Math.max(start, CigarUtils.consumedReferenceBases(cigar) + start - 1));
            this.startInAssembledContig = 1 + CigarUtils.leftHardClippedBases(cigar);
            this.endInAssembledContig = CigarUtils.calculateReadLength(cigar)
                    - CigarUtils.leftHardClippedBases(cigar);
            this.mapQual = mappingQuality;
            this.mismatches = mismatches;
            this.alignmentScore = alignmentScore;
            this.forwardStrand = forwardStrand;
            this.cigarAlong5to3DirectionOfContig = cigar;
        }

        @VisibleForTesting
        public AlignmentInterval(final SAMRecord samRecord) {
            final boolean isMappedReverse = samRecord.getReadNegativeStrandFlag();
            this.referenceInterval = new SimpleInterval(samRecord);
            this.startInAssembledContig = getAlignmentStartInOriginalContig(samRecord);
            this.endInAssembledContig = getAlignmentEndInOriginalContig(samRecord);

            this.cigarAlong5to3DirectionOfContig = isMappedReverse ? CigarUtils.invertCigar(samRecord.getCigar()) : samRecord.getCigar();
            this.forwardStrand = !isMappedReverse;
            this.mapQual = samRecord.getMappingQuality();
            final Integer nMismatches = samRecord.getIntegerAttribute("NM");
            this.mismatches = nMismatches==null ? SVConstants.DiscoveryStepConstants.MISSING_NM : nMismatches;
            this.alignmentScore = Optional.of(samRecord.getIntegerAttribute(SAMTag.AS.name())).orElse(MISSING_AS);
        }

        /**
         * Construct an alignment interval that reflects on the mapping properties of a {@link GATKRead} instance.
         * @param read
         */
        public AlignmentInterval(final GATKRead read) {
            final boolean isMappedReverse = read.isReverseStrand();
            this.referenceInterval = new SimpleInterval(read);
            this.startInAssembledContig = read.getFirstAlignedReadPosition();
            this.endInAssembledContig = read.getLastAlignedReadPosition();
            this.cigarAlong5to3DirectionOfContig = isMappedReverse ? CigarUtils.invertCigar(read.getCigar()) : read.getCigar();
            this.forwardStrand = !isMappedReverse;
            this.mapQual = read.getMappingQuality();
            final Integer nMismatches = read.getAttributeAsInteger(SAMTag.NM.name());
            this.mismatches = nMismatches==null ? SVConstants.DiscoveryStepConstants.MISSING_NM : nMismatches;
            this.alignmentScore = Optional.of(read.getAttributeAsInteger(SAMTag.AS.name())).orElse(MISSING_AS);
        }

        @VisibleForTesting
        public AlignmentInterval(final BwaMemAlignment alignment, final List<String> refNames, final int unclippedContigLength) {
            Utils.nonNull(alignment);
            if (alignment.getRefId() < 0) {
                throw new IllegalArgumentException("the input alignment must be mapped");
            }
            Utils.nonNull(refNames);
            this.referenceInterval = new SimpleInterval(refNames.get(alignment.getRefId()), alignment.getRefStart()+1, alignment.getRefEnd()); // +1 because the BwaMemAlignment class has 0-based coordinate system
            this.forwardStrand = (alignment.getSamFlag()& SAMFlag.READ_REVERSE_STRAND.intValue())==0;
            this.cigarAlong5to3DirectionOfContig = forwardStrand ? TextCigarCodec.decode(alignment.getCigar()) : CigarUtils.invertCigar(TextCigarCodec.decode(alignment.getCigar()));
            Utils.validateArg(cigarAlong5to3DirectionOfContig.getReadLength() + SVVariantDiscoveryUtils.getTotalHardClipping(cigarAlong5to3DirectionOfContig)
                    == unclippedContigLength, "contig length provided in constructor and inferred length by computation are different: " + unclippedContigLength + "\t" + alignment.getCigar().toString());

            this.mapQual = Math.max(SAMRecord.NO_MAPPING_QUALITY, alignment.getMapQual()); // BwaMemAlignment has negative mapQ for unmapped sequences, not the same as its SAMRecord conversion (see BwaMemAlignmentUtils.applyAlignment())
            this.mismatches = alignment.getNMismatches();
            this.alignmentScore = alignment.getAlignerScore();
            if ( forwardStrand ) {
                this.startInAssembledContig = alignment.getSeqStart() + 1;
                this.endInAssembledContig = alignment.getSeqEnd();
            } else {
                this.startInAssembledContig = unclippedContigLength - alignment.getSeqEnd() + 1;
                this.endInAssembledContig = unclippedContigLength - alignment.getSeqStart();
            }
        }

        public AlignmentInterval(final SimpleInterval referenceInterval, final int startInAssembledContig, final int endInAssembledContig,
                                 final Cigar cigarAlong5to3DirectionOfContig, final boolean forwardStrand, final int mapQual, final int mismatches, final int alignmentScore) {
            this.referenceInterval = referenceInterval;
            this.startInAssembledContig = startInAssembledContig;
            this.endInAssembledContig = endInAssembledContig;

            this.cigarAlong5to3DirectionOfContig = cigarAlong5to3DirectionOfContig;

            this.forwardStrand = forwardStrand;
            this.mapQual = mapQual;
            this.mismatches = mismatches;
            this.alignmentScore = alignmentScore;
        }

        static int getAlignmentStartInOriginalContig(final SAMRecord samRecord) {
            return SVVariantDiscoveryUtils.getNumClippedBases(!samRecord.getReadNegativeStrandFlag(), samRecord.getCigar()) + 1;
        }

        static int getAlignmentEndInOriginalContig(final SAMRecord samRecord) {
            final Cigar cigar = samRecord.getCigar();
            return cigar.getReadLength() + SVVariantDiscoveryUtils.getTotalHardClipping(cigar) - SVVariantDiscoveryUtils.getNumClippedBases(samRecord.getReadNegativeStrandFlag(), cigar);
        }

        public AlignmentInterval(final Kryo kryo, final Input input) {
            final String chr = input.readString();
            final int refStart = input.readInt(),
                    refEnd = input.readInt();
            referenceInterval = new SimpleInterval(chr, refStart, refEnd);
            startInAssembledContig = input.readInt();
            endInAssembledContig = input.readInt();
            cigarAlong5to3DirectionOfContig = TextCigarCodec.decode(input.readString());
            forwardStrand = input.readBoolean();
            mapQual = input.readInt();
            mismatches = input.readInt();
            alignmentScore = input.readInt();
        }

        public void serialize(final Kryo kryo, final Output output) {
            output.writeString(referenceInterval.getContig());
            output.writeInt(referenceInterval.getStart());
            output.writeInt(referenceInterval.getEnd());
            output.writeInt(startInAssembledContig);
            output.writeInt(endInAssembledContig);
            output.writeString(TextCigarCodec.encode(cigarAlong5to3DirectionOfContig));
            output.writeBoolean(forwardStrand);
            output.writeInt(mapQual);
            output.writeInt(mismatches);
            output.writeInt(alignmentScore);
        }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<AlignmentInterval> {
            @Override
            public void write( final Kryo kryo, final Output output, final AlignmentInterval alignmentInterval){
                alignmentInterval.serialize(kryo, output);
            }

            @Override
            public AlignmentInterval read(final Kryo kryo, final Input input, final Class<AlignmentInterval> clazz ) {
                return new AlignmentInterval(kryo, input);
            }
        }

        public static final String PACKED_STRING_REP_SEPARATOR = "_";
        /**
         * @return  A packed String representation of this alignment interval; intended for debugging or annotation usage (both requires compactified message).
         */
        public String toPackedString() {
            return String.join(PACKED_STRING_REP_SEPARATOR, String.valueOf(startInAssembledContig), String.valueOf(endInAssembledContig),
                    referenceInterval.toString(), (forwardStrand ? "+" : "-"),
                    TextCigarCodec.encode(cigarAlong5to3DirectionOfContig), String.valueOf(mapQual), String.valueOf(mismatches));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            AlignmentInterval that = (AlignmentInterval) o;

            if (startInAssembledContig != that.startInAssembledContig) return false;
            if (endInAssembledContig != that.endInAssembledContig) return false;
            if (forwardStrand != that.forwardStrand) return false;
            if (mapQual != that.mapQual) return false;
            if (mismatches != that.mismatches) return false;
            if (!referenceInterval.equals(that.referenceInterval)) return false;
            return cigarAlong5to3DirectionOfContig.equals(that.cigarAlong5to3DirectionOfContig);
        }

        @Override
        public int hashCode() {
            int result = referenceInterval.hashCode();
            result = 31 * result + startInAssembledContig;
            result = 31 * result + endInAssembledContig;
            result = 31 * result + cigarAlong5to3DirectionOfContig.hashCode();
            result = 31 * result + (forwardStrand ? 1 : 0);
            result = 31 * result + mapQual;
            result = 31 * result + mismatches;
            return result;
        }


        /**
         * Returns a {@link SAMRecord} instance that reflects this alignment interval given the
         * output {@link SAMFileHeader} and the enclosing {@link AlignedContig}.
         *
         * @param header the returned record header.
         * @param contig the enclosing contig.
         * @param hardClip whether clippings must be hard ones.
         * @return never {@code null}.
         */
        public SAMRecord convertToSAMRecord(final SAMFileHeader header, final AlignedContig contig, final boolean hardClip) {
            Utils.nonNull(header, "the input header cannot be null");
            Utils.nonNull(contig, "the input contig cannot be null");
            final SAMRecord result = new SAMRecord(header);

            result.setReadName(contig.contigName);
            result.setReadPairedFlag(false);
            result.setReadNegativeStrandFlag(!forwardStrand);

            // taking care of the bases;
            final byte[] bases = hardClip ? Arrays.copyOfRange(contig.contigSequence, startInAssembledContig - 1, endInAssembledContig) : contig.contigSequence.clone();
            if (!forwardStrand) {
                SequenceUtil.reverseComplement(bases);
            }
            result.setReadBases(bases);

            // taking care of the cigar.
            final Cigar cigar = forwardStrand ? this.cigarAlong5to3DirectionOfContig :
                    CigarUtils.invertCigar(this.cigarAlong5to3DirectionOfContig);

            result.setCigar(hardClip ? CigarUtils.hardClip(cigar) : CigarUtils.softClip(cigar));

            result.setReferenceName(referenceInterval.getContig());
            result.setAlignmentStart(referenceInterval.getStart());
            if (mapQual >= 0) {
                result.setMappingQuality(this.mapQual);
            }
            if (mismatches != -1) {
                result.setAttribute(SAMTag.NM.name(), mismatches);
            }
            if (alignmentScore != -1) {
                result.setAttribute(SAMTag.AS.name(), alignmentScore);
            }
            return result;
        }
    }

    public AlignedAssembly(final int assemblyId, final List<AlignedContig> alignedContigs) {
        this.assemblyId = assemblyId;
        this.alignedContigs = alignedContigs;
    }

    @VisibleForTesting
    private AlignedAssembly(final Kryo kryo, final Input input) {
        this.assemblyId = input.readInt();

        final int nContigs = input.readInt();
        alignedContigs = new ArrayList<>(nContigs);
        for(int contigIdx = 0; contigIdx < nContigs; ++contigIdx) {
            alignedContigs.add(new AlignedContig(kryo, input));
        }
    }

    @VisibleForTesting
    private void serialize(final Kryo kryo, final Output output) {
        output.writeInt(assemblyId);

        output.writeInt(alignedContigs.size());
        for(final AlignedContig alignedContig : alignedContigs) {
            alignedContig.serialize(kryo, output);
        }
    }

    public static final class Serializer extends com.esotericsoftware.kryo.Serializer<AlignedAssembly> {
        @Override
        public void write( final Kryo kryo, final Output output, final AlignedAssembly alignedAssembly){
            alignedAssembly.serialize(kryo, output);
        }

        @Override
        public AlignedAssembly read(final Kryo kryo, final Input input, final Class<AlignedAssembly> clazz ) {
            return new AlignedAssembly(kryo, input);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AlignedAssembly that = (AlignedAssembly) o;

        if (assemblyId != that.assemblyId) return false;
        return alignedContigs.equals(that.alignedContigs);
    }

    @Override
    public int hashCode() {
        int result = assemblyId;
        result = 31 * result + alignedContigs.hashCode();
        return result;
    }
}
