package org.broadinstitute.hellbender.tools.spark.sv.evidence;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.netflix.servo.util.VisibleForTesting;
import htsjdk.samtools.SAMFileHeader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.StructuralVariationSparkProgramGroup;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvType;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVUtils;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.spark.utils.IntHistogram;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.*;
import java.util.*;

import static org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection.FindBreakpointEvidenceSparkArgumentCollection;

@CommandLineProgramProperties(summary="Find regions likely to contain small indels due to fragment length anomalies.",
        oneLineSummary="Find regions containing small indels.",
        programGroup=StructuralVariationSparkProgramGroup.class)
public final class FindSmallIndelRegions extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @ArgumentCollection
    private static final FindBreakpointEvidenceSparkArgumentCollection params =
                new FindBreakpointEvidenceSparkArgumentCollection();

    @Argument(doc = "sam file for aligned contigs", shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME)
    private String outputFile;

    @Argument(doc = "Five-column text file indicating location of true SVs.",
                fullName = "truthFileName", optional = true)
    private static String truthFile;

    @Argument(doc = "Directory into which to write CDFs.", fullName = "cdfDir", optional = true)
    private static String cdfDir;

    private static final int EVIDENCE_SIZE_GUESS = 1000;

    @Override
    public boolean requiresReads()
    {
        return true;
    }

    @Override
    protected void runTool( final JavaSparkContext ctx ) {

        final SAMFileHeader header = getHeaderForReads();
        final JavaRDD<GATKRead> allReads = getUnfilteredReads();
        final Set<Integer> crossContigIgnoreSet = Collections.emptySet();
        final int maxTrackedFragmentLength = params.maxTrackedFragmentLength;
        final SVReadFilter filter = new SVReadFilter(params);
        final ReadMetadata readMetadata =
                new ReadMetadata(crossContigIgnoreSet, header, maxTrackedFragmentLength, allReads, filter);
        if ( params.metadataFile != null ) {
            ReadMetadata.writeMetadata(readMetadata, params.metadataFile);
        }
        final Broadcast<ReadMetadata> broadcastReadMetadata = ctx.broadcast(readMetadata);
        final SVIntervalTree<SVTypeLen> trueIntervals = readTruthFile(truthFile, readMetadata.getContigNameMap());
        final Broadcast<SVIntervalTree<SVTypeLen>> broadcastTrueIntervals = ctx.broadcast(trueIntervals);
        final String cdfDirname = cdfDir;
        if ( cdfDir != null ) {
            for ( Map.Entry<String, LibraryStatistics> entry : readMetadata.getAllLibraryStatistics().entrySet() ) {
                final String fileName = cdfDir + "/" + entry.getKey() + ".allreads.cdf";
                try ( final BufferedWriter writer =
                              new BufferedWriter(new OutputStreamWriter(BucketUtils.createFile(fileName))) ) {
                    final IntHistogram.CDF cdf = entry.getValue().getCDF();
                    writer.write(String.format("#POP\t%d\n", cdf.getTotalObservations()));
                    final int cdfLen = cdf.size();
                    for ( int idx = 0; idx != cdfLen; ++idx ) {
                        writer.write(String.format("%.3f\n", cdf.getFraction(idx)));
                    }
                } catch ( final IOException ioe ) {
                    throw new UserException("Unable to write sample CDF " + fileName, ioe);
                }
            }
        }
        allReads
            .mapPartitions(readItr -> {
                final List<String> statList = new ArrayList<>(EVIDENCE_SIZE_GUESS);
                final Finder finder = new Finder(broadcastReadMetadata.value(), filter,
                                                    broadcastTrueIntervals.value(), cdfDirname);
                while ( readItr.hasNext() ) {
                    finder.testReadAndGatherStatus(readItr.next(), statList);
                }
                finder.getStatus(statList);
                return statList.iterator();
            })
            .saveAsTextFile(outputFile);
    }

    @DefaultSerializer(SVTypeLen.Serializer.class)
    public final static class SVTypeLen {
        private SvType.TYPES type;
        private int len;

        public SVTypeLen( final String svType, final int len ) {
            this.type = SvType.TYPES.valueOf(svType);
            this.len = len;
        }

        public SVTypeLen( final Kryo kryo, final Input input ) {
            type = SvType.TYPES.values()[input.readInt()];
            len = input.readInt();
        }

        public void serialize( final Kryo kryo, final Output output ) {
            output.writeInt(type.ordinal());
            output.writeInt(len);
        }

        public SvType.TYPES getType() { return type; }
        public int getLen() { return len; }

        public static final class Serializer extends com.esotericsoftware.kryo.Serializer<SVTypeLen> {
            @Override
            public void write( final Kryo kryo, final Output output, final SVTypeLen interval ) {
                interval.serialize(kryo, output);
            }

            @Override
            public SVTypeLen read( final Kryo kryo, final Input input, final Class<SVTypeLen> klass ) {
                return new SVTypeLen(kryo, input);
            }
        }
    }

    private static SVIntervalTree<SVTypeLen> readTruthFile( final String truthFile,
                                                            final Map<String,Integer> contigNameToIDMap ) {
        final SVIntervalTree<SVTypeLen> eventMap = new SVIntervalTree<>();
        if ( truthFile == null ) return eventMap;
        try ( final BufferedReader reader = new BufferedReader(new InputStreamReader(BucketUtils.openFile(truthFile))) ) {
            String line;
            while ( (line = reader.readLine()) != null ) {
                String[] tokens = line.split("\t");
                if ( tokens.length != 5 ) {
                    throw new IOException("This line has only " + tokens.length + " columns: " + line);
                }
                final Integer contigID = contigNameToIDMap.get(tokens[0]);
                if ( contigID == null ) {
                    throw new IOException("This line has a bogus contig name: " + line);
                }
                final int start;
                try {
                    start = Integer.parseInt(tokens[1]);
                } catch ( final NumberFormatException nfe ) {
                    throw new IOException("This line has a bogus coordinate: " + line);
                }
                final int end;
                try {
                    int tmpEnd = Integer.parseInt(tokens[2]);
                    if ( tmpEnd != start ) tmpEnd += 1;
                    end = tmpEnd;
                } catch ( final NumberFormatException nfe ) {
                    throw new IOException("This line has a bogus coordinate: " + line);
                }
                final int len;
                try {
                    len = Integer.parseInt(tokens[4]);
                } catch ( final NumberFormatException nfe ) {
                    throw new IOException("This line has a bogus coordinate: " + line);
                }
                eventMap.put(new SVInterval(contigID,start,end),new SVTypeLen(tokens[3],len));
            }
        } catch ( final IOException ioe ) {
            throw new GATKException("Can't read truth file " + truthFile, ioe);
        }
        return eventMap;
    }

    public static final class Finder {
        private static final int BLOCK_SIZE = 250;
        private static final int EVIDENCE_WEIGHT = 5;
        private static final float KS_SIGNIFICANCE = 3.1E-4f;

        // When multiplied by the library read frequency this number gives an upper bound on the number of observations
        // in a window.  we don't want to evaluate windows with too many observations because they're mostly due
        // to mapping defects causing huge pileups.
        // Here's how this number is actually derived:
        // WINDOW_SIZE = 2 * BLOCK_SIZE
        // EXPECTED_READS_PER_WINDOW = WINDOW_SIZE * library read frequency
        // Deflate according to a heuristic derived from the shape of the actual distribution -- due to mapping
        // error there's a shift down in the median from all the huge pileups sucking reads away.
        // This may need to be revisited whenever we change the mapper.
        // ACTUAL_MEDIAN_READS_PER_WINDOW = EXPECTED_READS_PER_WINDOW / 2
        // Finally, we put, as a maximum, a copy number of 5 (most things with copy numbers that high are mapping
        // artifacts).  that's 2.5x the expected number of reads because that's a diploid number.
        private static final float MAX_OBSERVATIONS_SCALE = BLOCK_SIZE*2.5f;

        private final ReadMetadata readMetadata;
        private final SVReadFilter filter;
        private final SVIntervalTree<SVTypeLen> trueIntervals;
        private final String cdfDir;

        // We're calculating a K-S statistic over 500-base windows, but the windows are tiled every 250 bases.
        // So what we do is to keep a pair of histograms for each library: one for first half of the window, and one
        // for the second half.
        // The reads are coordinate sorted, and whenever the next read presented to the test method crosses out of the
        // current window, it's time to do a K-S test.
        // We add the 2nd-half histogram to the 1st-half histogram, so that the 1st-half histogram now has the complete
        // data for the entire window.  We do the K-S test, and then zero the 1st-half histogram.
        // At this point the 2nd-half histogram becomes the 1st-half histogram for the next window, and the newly
        // cleared 1st-half histogram becomes the 2nd-half histogram for the next window.
        // We swap roles simply by toggling fillIdx between 0 and 1 each time we cross a boundary.
        // So in the code that follows, fillIdx points to the member of the pair that is currently accumulating data --
        // i.e., the 2nd-half histogram.  The temporary variable oldIdx in the checkHistograms method below points to
        // the other member of the pair -- i.e., the 1st-half histogram.
        private final Map<String, IntHistogram[]> libraryToHistoPairMap;
        private int fillIdx;
        // curContig+curEnd mark the genomic location of the end of the current window
        private String curContig;
        private int curEnd;

        public Finder( final ReadMetadata readMetadata, final SVReadFilter filter ) {
            this(readMetadata, filter, new SVIntervalTree<>(), null);
        }

        public Finder( final ReadMetadata readMetadata,
                       final SVReadFilter filter,
                       final SVIntervalTree<SVTypeLen> trueIntervals,
                       final String cdfDir ) {
            this.readMetadata = readMetadata;
            this.filter = filter;
            this.trueIntervals = trueIntervals;
            this.cdfDir = cdfDir;
            final Map<String, LibraryStatistics> libraryStatisticsMap = readMetadata.getAllLibraryStatistics();
            libraryToHistoPairMap = new HashMap<>(SVUtils.hashMapCapacity(libraryStatisticsMap.size()));
            libraryStatisticsMap.forEach( (libName, stats) -> {
                final IntHistogram[] pair = new IntHistogram[2];
                pair[0] = stats.createEmptyHistogram();
                pair[1] = stats.createEmptyHistogram();
                libraryToHistoPairMap.put(libName, pair);
            });
            curContig = null;
            curEnd = 0;
            fillIdx = 0;
        }

        public void testReadAndGatherEvidence( final GATKRead read, final List<BreakpointEvidence> evidenceList ) {
            if ( !filter.isTemplateLenTestable(read) ) return;

            final boolean sameContig;
            if ( !(sameContig = read.getContig().equals(curContig)) || read.getStart() >= curEnd ) {
                checkHistograms(evidenceList);
                advanceBlock(sameContig, read);
            }
            final IntHistogram[] histoPair =
                    libraryToHistoPairMap.get(readMetadata.getLibraryName(read.getReadGroup()));
            histoPair[fillIdx].addObservation(Math.abs(read.getFragmentLength()));
        }

        public void checkHistograms( final List<BreakpointEvidence> evidenceList ) {
            if ( curContig == null ) return;

            final int oldIdx = fillIdx ^ 1; // bit magic sets oldIdx to 1 if fillIdx is 0, and vice versa
            final int start = Math.max(1, curEnd - 2 * BLOCK_SIZE);
            final SVInterval curInterval = new SVInterval(readMetadata.getContigID(curContig), start, curEnd);
            for ( final Map.Entry<String, IntHistogram[]> entry : libraryToHistoPairMap.entrySet() ) {
                final IntHistogram[] histoPair = entry.getValue();
                final IntHistogram oldHisto = histoPair[oldIdx];
                oldHisto.addObservations(histoPair[fillIdx]);
                final long readCount = oldHisto.getTotalObservations();
                if ( readCount > 0 &&
                        readMetadata.getLibraryStatistics(entry.getKey()).getCDF()
                                .isDifferentByKSStatistic(oldHisto, KS_SIGNIFICANCE) ) {
                    evidenceList.add(
                        new BreakpointEvidence.TemplateSizeAnomaly(curInterval, EVIDENCE_WEIGHT, (int)readCount));
                }
                oldHisto.clear();
            }
        }

        public void testReadAndGatherStatus( final GATKRead read, final List<String> statusList ) {
            if ( !filter.isTemplateLenTestable(read) ) return;

            final boolean sameContig;
            if ( !(sameContig = read.getContig().equals(curContig)) || read.getStart() >= curEnd ) {
                getStatus(statusList);
                advanceBlock(sameContig, read);
            }
            final IntHistogram[] histoPair =
                    libraryToHistoPairMap.get(readMetadata.getLibraryName(read.getReadGroup()));
            histoPair[fillIdx].addObservation(Math.abs(read.getFragmentLength()));
        }

        public void getStatus( final List<String> statusList ) {
            if ( curContig == null ) return;
            final int oldIdx = fillIdx ^ 1; // bit magic sets oldIdx to 1 if fillIdx is 0, and vice versa
            final int start = Math.max(1, curEnd - 2 * BLOCK_SIZE);
            for ( final Map.Entry<String, IntHistogram[]> entry : libraryToHistoPairMap.entrySet() ) {
                final IntHistogram[] histoPair = entry.getValue();
                final IntHistogram oldHisto = histoPair[oldIdx];
                oldHisto.addObservations(histoPair[fillIdx]);
                final SVInterval curInterval = new SVInterval(readMetadata.getContigID(curContig), start, curEnd);
                final Iterator<SVIntervalTree.Entry<SVTypeLen>> overlapperItr = trueIntervals.overlappers(curInterval);
                final String libName = entry.getKey();
                final long counts = oldHisto.getTotalObservations();
                if ( counts == 0L )
                    statusList.add(String.format("%s\t%s\t%s\t%d\t%d\t0",
                                            libName, overlapperItr.hasNext()?"FN":"TN", curContig, start, curEnd));
                else {
                    final LibraryStatistics stats = readMetadata.getLibraryStatistics(libName);
                    final IntHistogram.CDF cdf = stats.getCDF();
                    final IntHistogram.CDF.KSData ksData = cdf.evalKS(oldHisto);
                    final long maxObservations = (long)(MAX_OBSERVATIONS_SCALE*stats.getReadStartFrequency());
                    final boolean isCalledInterval =
                            ksData.getSignificance() <= KS_SIGNIFICANCE &&
                            oldHisto.getTotalObservations() <= maxObservations;
                    final boolean isTrue = overlapperItr.hasNext();
                    final String truthStatus = (isCalledInterval==isTrue ? "T" : "F") +
                            (isCalledInterval ? "P" : "N");
                    final int relativeLocation = stats.getMedian() - ksData.getLocationOfMaxDiff();
                    final int firstOverlappingEventSize;
                    if ( !overlapperItr.hasNext() ) firstOverlappingEventSize = 0;
                    else {
                        final SVTypeLen overlappingTypeLen = overlapperItr.next().getValue();
                        switch ( overlappingTypeLen.getType() ) {
                            case DEL: firstOverlappingEventSize = -overlappingTypeLen.getLen(); break;
                            case INS: firstOverlappingEventSize = overlappingTypeLen.getLen(); break;
                            default: firstOverlappingEventSize = 0; break;
                        }
                    }
                    final String status = String.format("%s\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%.2f\t%.2f\t%.1e",
                                                        libName, truthStatus, curContig, start, curEnd, counts,
                                                        relativeLocation, firstOverlappingEventSize,
                                                        ksData.getMaxDiff(), ksData.getMaxArea(),
                                                        ksData.getSignificance());
                    statusList.add(status);
                    if ( cdfDir != null && (isCalledInterval || isTrue) ) {
                        final String fileName =
                                cdfDir + "/" + libName + "." + curContig + "." + start + "." + truthStatus + ".cdf";
                        dumpCDF(fileName, oldHisto.getCDF(), status, trueIntervals.overlappers(curInterval));
                    }
                }
                oldHisto.clear();
            }
        }

        private void advanceBlock( final boolean sameContig, final GATKRead read ) {
            final int oldIdx = fillIdx;
            fillIdx ^= 1; // bit magic to switch halves
            curEnd += BLOCK_SIZE;
            if ( !sameContig || read.getStart() >= curEnd ) {
                curContig = read.getContig();
                curEnd = read.getStart() + BLOCK_SIZE;
                // clear 1st-half window when we switch contigs or blast forward due to a gap in coverage
                libraryToHistoPairMap.values().forEach(histPair -> histPair[oldIdx].clear());
            }
        }

        private void dumpCDF( final String fileName, final IntHistogram.CDF sampleCDF, final String summaryStatus,
                              final Iterator<SVIntervalTree.Entry<SVTypeLen>> overlappers ) {
            try ( final BufferedWriter writer =
                          new BufferedWriter(new OutputStreamWriter(BucketUtils.createFile(fileName))) ) {
                writer.write(String.format("#CDF\t%s\n", summaryStatus));
                while ( overlappers.hasNext() ) {
                    final SVIntervalTree.Entry<SVTypeLen> entry = overlappers.next();
                    final SVInterval overlappingInterval = entry.getInterval();
                    final SVTypeLen typeLen = entry.getValue();
                    final String overlapper = String.format("#OVL\t%s\t%d\t%d\t%s\t%d\n",
                            readMetadata.getContigName(overlappingInterval.getContig()),
                            overlappingInterval.getStart(), overlappingInterval.getEnd(),
                            typeLen.getType().toString(), typeLen.getLen());
                    writer.write(overlapper);
                }
                final int histoLen = sampleCDF.size();
                for ( int idx = 0; idx != histoLen; ++idx ) {
                    writer.write(String.format("%.3f\n", sampleCDF.getFraction(idx)));
                }
            } catch ( final IOException ioe ) {
                throw new UserException("Unable to write CDF " + fileName, ioe);
            }
        }

        @VisibleForTesting
        Map<String, IntHistogram[]> getLibraryToHistoPairMap() { return libraryToHistoPairMap; }
    }
}
