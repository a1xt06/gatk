package org.broadinstitute.hellbender.tools.exome;

import htsjdk.samtools.util.Locatable;
import org.apache.commons.collections4.ListUtils;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.IndexRange;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.tsv.TableColumnCollection;
import org.broadinstitute.hellbender.utils.tsv.TableReader;
import org.broadinstitute.hellbender.utils.tsv.TableUtils;
import org.broadinstitute.hellbender.utils.tsv.TableWriter;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by David Benjamin 7/15/15
 */
public final class SegmentUtils {
    private static final String SAMPLE_COLUMN = "Sample";
    private static final String CONTIG_COLUMN = "Chromosome";
    private static final String START_COLUMN = "Start";
    private static final String END_COLUMN = "End";
    private static final String NUM_PROBES_COLUMN = "Num_Probes";
    private static final String MEAN_COLUMN = "Segment_Mean";
    private static final String CALL_COLUMN = "Segment_Call";
    private static final String MISSING_DATA = "NA";

    private SegmentUtils() {}

    /**
     * read a list of intervals without calls from a segfile with header:
     * Sample   Chromosome  Start  End Num_PRobes  Segment_Mean    Segment_Call
     */
    public static List<SimpleInterval> readIntervalsFromSegfile(final File segmentsFile) {
        try (final TableReader<SimpleInterval> reader = TableUtils.reader(segmentsFile,
                (columns, formatExceptionFactory) -> {
                    if (!columns.containsAll(SAMPLE_COLUMN, CONTIG_COLUMN, START_COLUMN, END_COLUMN)) {
                        throw formatExceptionFactory.apply("Bad header");
                    }
                    // return the lambda to translate dataLines into uncalled segments.
                    return (dataLine) -> new SimpleInterval(dataLine.get(CONTIG_COLUMN), dataLine.getInt(START_COLUMN), dataLine.getInt(END_COLUMN));
                })) {
            return reader.stream().collect(Collectors.toList());
        } catch (final IOException | UncheckedIOException e) {
            throw new UserException.CouldNotReadInputFile(segmentsFile, e);
        }
    }

    /**
     * read a list of segments (with calls, if possible) from a file.  Creates a list of modeled segments
     *
     * Note Segment_Call is optional.  If not present, the modeled segments will be populated with a blank value.
     */
    public static List<ModeledSegment> readModeledSegmentsFromSegfile(final File segmentsFile) {
        try (final TableReader<ModeledSegment> reader = TableUtils.reader(segmentsFile,
                (columns, formatExceptionFactory) -> {
                    // Note that Segment_Call is optional
                    if (!columns.containsAll(SAMPLE_COLUMN, CONTIG_COLUMN, START_COLUMN, END_COLUMN, NUM_PROBES_COLUMN,
                            MEAN_COLUMN)) {
                        throw formatExceptionFactory.apply("Bad header");
                    }
                    // return the lambda to translate dataLines into called segments.
                    return (dataLine) -> new ModeledSegment(
                            new SimpleInterval(dataLine.get(CONTIG_COLUMN), dataLine.getInt(START_COLUMN), dataLine.getInt(END_COLUMN)),
                            dataLine.get(CALL_COLUMN, ModeledSegment.NO_CALL),
                            dataLine.getLong(NUM_PROBES_COLUMN),
                            dataLine.getDouble(MEAN_COLUMN));
                })) {
            return reader.stream().collect(Collectors.toList());
        } catch (final IOException | UncheckedIOException e) {
            throw new UserException.CouldNotReadInputFile(segmentsFile, e);
        }
    }

    /**
     * the mean of all overlapping targets' coverages
     *
     * @throws IllegalStateException if overlapping targets have not been assigned or if no overlapping targets were found.
     */
    public static double meanTargetCoverage(final Locatable loc, final TargetCollection<TargetCoverage> targets) {
        Utils.nonNull(loc, "Can't get mean coverage of null segment.");
        Utils.nonNull(targets, "Mean target coverage requires non-null targets collection.");

        final List<TargetCoverage> myTargets = targets.targets(loc);

        if (myTargets.size() == 0) {
            throw new IllegalStateException("Empty segment -- no overlapping targets.");
        }
        return myTargets.stream().mapToDouble(TargetCoverage::getCoverage).average().getAsDouble();
    }
    
    
    /**
     *
     * @param interval some genomic region
     * @param targets collection of target genomic regions
     * @param snps collection of SNP sites represented as length-1 intervals
     *
     * @return the smallest interval that contains the entirety (start to end) of every interval in {@code targets} and
     * {@code snps} that overlaps {@code interval} at all.  If {@code interval} overlaps no targets or SNPs, return a length-1 interval
     * at the start of {@code interval}.
     *
     * For example, if {@code interval} is 1:100-200 and the only target or snp it overlaps is 1:60-180, then the result
     * is 1:60-180.
     *
     * Note that if {@code targets} contains overlapping targets we can get an inconsistent result where
     * parsimoniousInterval returns an interval that is not parsimonious by its own definition.  For example, if {@code targets}
     * contains 1:40-90 and 1:80-110 and {@code interval} is 1:100-200, then parsimoniousInterval yields 1:80-110, which
     * falls in the middle of a target.  Since sites in {@code snps} are length-1, there is no such concern for them.
     */
    public static SimpleInterval parsimoniousInterval(final Locatable interval, final TargetCollection<? extends Locatable> targets,
                                                                     final TargetCollection<? extends Locatable> snps) {
        Utils.nonNull(interval, "Interval may not be null");
        Utils.nonNull(targets, "Targets may not be null");
        Utils.nonNull(snps, "SNPs may not be null");

        final IndexRange targetRange = targets.indexRange(interval);
        final IndexRange snpRange = snps.indexRange(interval);

        final int targetsInInterval = targetRange.size();
        final int snpsInInterval = snpRange.size();

        int start;
        int end;

        if ( targetsInInterval == 0 && snpsInInterval == 0) {
            start = interval.getStart();
            end = interval.getStart();
        } else if (targetsInInterval == 0) {
            start = snps.target(snpRange.from).getStart();
            end = snps.target(snpRange.to - 1).getEnd();
        } else if (snpsInInterval == 0) {
            start = targets.target(targetRange.from).getStart();
            end = targets.target(targetRange.to - 1).getEnd();
        } else {
            start = Math.min(targets.target(targetRange.from).getStart(), snps.target(snpRange.from).getStart());
            end = Math.max(targets.target(targetRange.to - 1).getEnd(), snps.target(snpRange.to - 1).getEnd());
        }
        return new SimpleInterval(interval.getContig(), start, end);
    }

    /**
     * Naive segment union consisting of the union of all breakpoints with slight touching-up.
     *
     * @param targetSegments a list of segment intervals defined by segmentation of {@code targets}
     * @param snpSegments a list of segment intervals defined by segmentation of {@code snps}
     * @param targets a list of target intervals
     * @param snps a list of snp sites represented as intervals
     * @return A list of segments defined by the set of all breakpoints in either {@code targetSegments} or {@code snpSegments}, with
     * segments containing no targets or snps filtered out, and with {@code parsimoniousInterval} applied.
     *
     * Example: suppose targets are 1:20-30, 1:40-50, 1:60-70; snps are 1:25-25, 1:45-45, 1:65-65, targetSegments are
     * 1:15-55, 1:60-80; snpSegments are 1:25-65.  Then segment breakpoints are 15, 25, 55, 60, 65, 80.  Considering candidate
     * segments in order, we have 1:15-25, which becomes 1:20-30 under parsimoniousInterval.  Next we consider not 1:25-55 but
     * 1:31-55 in order not to overlap with the previous segment.  Under parsimoniousInterval this yields 1:40-50.  Next we consider
     * 1:51-60, which yields 1:60-70 under parsimoniousInterval.  We skip the candidate segment 1:60-65 because it ends before the
     * previously-added segment.  Finally we consider 1:71-80, which is empty and yields no segment.  The resulting
     * segments are therefore 1:20-30, 1:40-50, and 1:60-70.
     */
    public static List<SimpleInterval> segmentUnion(final List<? extends Locatable> targetSegments, final List<? extends Locatable> snpSegments,
                final TargetCollection<? extends Locatable> targets, final TargetCollection<? extends Locatable> snps) {
        Utils.nonNull(targetSegments, "Segment lists can't be null");
        Utils.nonNull(snpSegments, "Segment lists can't be null");
        Utils.nonNull(targets, "Targets may not be null");
        Utils.nonNull(snps, "SNPs may not be null");

        final SortedMap<String, ArrayList<Integer>> breakpoints = breakpointsByContig(ListUtils.union(targetSegments, snpSegments));
        final List<SimpleInterval> result = new ArrayList<>();
        for (final String contig : breakpoints.keySet()) {
            result.addAll(parsimoniousSegments(targets, snps, contig, breakpoints.get(contig)));
        }
        return result;
    }

    /**
     *
     * @param targets a list of target intervals
     * @param snps a list of snp sites represented as intervals
     * @param contig the contig
     * @param breaks the list of breakpoints within {@code contig}
     * @return a list of parsimonious intervals (see Javadoc for method {@code parsimoniousIntervals}) originating from
     * intervals defined by consecutive breakpoints, with empty intervals removed.
     */
    private static List<SimpleInterval> parsimoniousSegments(TargetCollection<? extends Locatable> targets,
            TargetCollection<? extends Locatable> snps, String contig, List<Integer> breaks) {
        final List<SimpleInterval> result = new ArrayList<>();
        /**
         * Form segments from breakpoints.  Note that with breakpoints a, b, c we can't form segments [a,b] and [b,c]
         * (inclusive) because locus b would be shared, or because parsimomiousInterval could extend the [a,b] segment.
         * Thus we begin the each segment from one past the end of the last added segment and invoke parsimoniousInterval
         * to get the correct behavior.
         */
        int nextStart = breaks.get(0);
        for (int i = 1; i < breaks.size(); i++) {
            final int end = breaks.get(i);
            if (end < nextStart) {  //this could happen if parsimoniousInterval enlarged the previous segment past the next breakpoint
                continue;
            }
            final SimpleInterval interval = new SimpleInterval(contig, nextStart, end);
            if (targets.targetCount(interval) > 0 || snps.targetCount(interval) > 0) {
                final SimpleInterval resultInterval = parsimoniousInterval(interval, targets, snps);
                result.add(resultInterval);
                nextStart = resultInterval.getEnd() + 1;
            }
        }
        return result;
    }

    /**
     * Given a collection of Locatables, make sorted lists of each contig's breakpoints -- the set of interval starts and ends
     *
     * @param intervals genomic intervals
     * @return a map whose keys are contigs and whose values are lists of breakpoints on that contig
     */
    private static SortedMap<String, ArrayList<Integer>> breakpointsByContig(final Collection<? extends Locatable> intervals) {
        final SortedMap<String, ArrayList<Integer>> breakpoints = new TreeMap<>();
        for (final Locatable interval : intervals) {
            final String contig = interval.getContig();

            ArrayList<Integer> list = breakpoints.get(contig);
            if ( list == null ) {
                breakpoints.put(contig, list = new ArrayList<>());
            }
            list.add(interval.getStart());
            list.add(interval.getEnd());
        }

        for (final ArrayList<Integer> list : breakpoints.values()) {
            Collections.sort(list);
        }

        return breakpoints;
    }

    /** Get difference between a segment mean and the overlapping targets.
     *
     * This will select the overlapping targets from the input collection, for your convenience.
     *
     * @param segment -- segment to use in calculating all difference of target CRs from the mean
     * @param targets -- targets.  Note that this method will use only the targets overlapping the given segment.
     * @return never {@code null}.  List of doubles of the difference, possibly empty.  Modifiable.
     */
    public static List<Double> segmentMeanTargetDifference(final ModeledSegment segment, final TargetCollection<TargetCoverage> targets) {
        Utils.nonNull(segment, "Can't count targets of null segment.");
        Utils.nonNull(targets, "Counting targets requires non-null targets collection.");

        final double segMean = segment.getSegmentMeanInCRSpace();
        final List<TargetCoverage> myTargets = targets.targets(segment);
        return myTargets.stream().map(t -> ( Math.pow(2, t.getCoverage()) - segMean)).collect(Collectors.toList());
    }

    /**
     * write a list of intervals with calls to file
     */
    public static void writeModeledSegmentsToSegfile(final File outFile, List<ModeledSegment> segments, final String sample) {
        try (final TableWriter<ModeledSegment> writer = TableUtils.writer(outFile,
                new TableColumnCollection(SAMPLE_COLUMN, CONTIG_COLUMN, START_COLUMN, END_COLUMN, NUM_PROBES_COLUMN,
                        MEAN_COLUMN, CALL_COLUMN),

                //lambda for filling an initially empty DataLine
                (ci, dataLine) -> {
                    dataLine.append(sample, ci.getContig()).append(ci.getStart(), ci.getEnd())
                            .append(ci.getOriginalProbeCount())
                            .append(ci.getSegmentMean())
                            .append(ci.getCall());
                })) {
            for (final ModeledSegment ci : segments) {
                if (ci == null) {
                    throw new IllegalArgumentException("Segments list contains a null.");
                }
                writer.writeRecord(ci);
            }
        } catch (final IOException e) {
            throw new UserException.CouldNotCreateOutputFile(outFile, e);
        }
    }
}