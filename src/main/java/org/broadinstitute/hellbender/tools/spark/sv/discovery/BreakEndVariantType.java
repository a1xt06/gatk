package org.broadinstitute.hellbender.tools.spark.sv.discovery;

import htsjdk.samtools.util.SequenceUtil;
import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.ArrayUtils;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.NovelAdjacencyReferenceLocations;
import org.broadinstitute.hellbender.tools.spark.sv.discovery.SvType;
import org.broadinstitute.hellbender.tools.spark.sv.utils.GATKSVVCFConstants;
import org.broadinstitute.hellbender.utils.SimpleInterval;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public abstract class BreakEndVariantType extends SvType {

    public static int BREAKEND_VARIANT_TYPE_LENGTH = -1;

    @Override
    public final String toString() {
        return GATKSVVCFConstants.BREAKEND_STR;
    }

    // see VCF spec 4.2 or 4.3 for BND format ALT allele field for SV
    BreakEndVariantType(final String id, final Map<String, String> typeSpecificExtraAttributes,
                        final byte[] bases, final boolean bracketPointsLeft, final SimpleInterval novelAdjRefLoc,
                        final boolean basesFirst) {
        super(id, constructAltAllele(bases, bracketPointsLeft, novelAdjRefLoc, basesFirst), BREAKEND_VARIANT_TYPE_LENGTH, typeSpecificExtraAttributes);
    }

    private static Allele constructAltAllele(final byte[] bases, final boolean bracketPointsLeft, final SimpleInterval novelAdjRefLoc,
                                             final boolean basesFirst) {
        final String s = bracketPointsLeft ? "]" + novelAdjRefLoc.getContig() + ":" + novelAdjRefLoc.getStart() + "]"
                                           : "[" + novelAdjRefLoc.getContig() + ":" + novelAdjRefLoc.getStart() + "[";
        return Allele.create( basesFirst ? new String(bases) + s  : s + new String(bases) );
    }

    static abstract class StrandSwitchBND extends BreakEndVariantType {

        StrandSwitchBND(final String id, final Map<String, String> typeSpecificExtraAttributes,
                        final byte[] bases, final boolean bracketPointsLeft, final SimpleInterval novelAdjRefLoc,
                        final boolean basesFirst){
            super(id, typeSpecificExtraAttributes, bases, bracketPointsLeft, novelAdjRefLoc, basesFirst);
        }


        static byte[] extractBasesForAltAllele(final NovelAdjacencyReferenceLocations narl, final boolean forUpstreamLoc,
                                                               final ReferenceMultiSource reference) {
            try {
                final byte[] ref = reference
                        .getReferenceBases(null, forUpstreamLoc ? narl.leftJustifiedLeftRefLoc :
                                narl.leftJustifiedRightRefLoc)
                        .getBases();
                final String ins = narl.complication.getInsertedSequenceForwardStrandRep();
                if (ins.isEmpty()) {
                    return ref;
                } else {
                    return forUpstreamLoc ? ArrayUtils.addAll(ref, ins.getBytes())
                            : ArrayUtils.addAll(ref, SequenceUtil.reverseComplement(ins).getBytes());
                }
            } catch (final IOException ioex) {
                throw new GATKException("Could not read reference for extracting reference bases.", ioex);
            }
        }

        static SimpleInterval getTheOtherRefLoc(final NovelAdjacencyReferenceLocations narl, final boolean forUpstreamLoc) {
            return forUpstreamLoc ? narl.leftJustifiedRightRefLoc : narl.leftJustifiedLeftRefLoc;
        }
    }

    static final class INV55BND extends StrandSwitchBND {

        /**
         * Technically, a strand switch breakpoint should have two VCF records, hence we also have {@code forUpstreamLoc}.
         */
        INV55BND(final NovelAdjacencyReferenceLocations narl, final boolean forUpstreamLoc,
                 final ReferenceMultiSource reference) {
            super(getIDString(narl, forUpstreamLoc),
                    Collections.singletonMap(GATKSVVCFConstants.INV55, ""),
                    extractBasesForAltAllele(narl, forUpstreamLoc, reference), true,
                    getTheOtherRefLoc(narl, forUpstreamLoc), true);
        }

        private static String getIDString(final NovelAdjacencyReferenceLocations narl, final boolean forUpstreamLoc) {

            return GATKSVVCFConstants.BREAKEND_STR + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    GATKSVVCFConstants.INV55 + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    narl.leftJustifiedLeftRefLoc.getContig() + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    narl.leftJustifiedLeftRefLoc.getEnd() + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    narl.leftJustifiedRightRefLoc.getStart() + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    (forUpstreamLoc ? "1" : "2") ;
        }
    }

    static final class INV33BND extends StrandSwitchBND {

        /**
         * Technically, a strand switch breakpoint should have two VCF records, hence we also have {@code forUpstreamLoc}.
         */
        INV33BND(final NovelAdjacencyReferenceLocations narl, final boolean forUpstreamLoc,
                 final ReferenceMultiSource reference) {
            super(getIDString(narl, forUpstreamLoc),
                    Collections.singletonMap(GATKSVVCFConstants.INV33, ""),
                    extractBasesForAltAllele(narl, forUpstreamLoc, reference), false,
                    getTheOtherRefLoc(narl, forUpstreamLoc), false);
        }

        private static String getIDString(final NovelAdjacencyReferenceLocations narl, final boolean forUpstreamLoc) {

            return GATKSVVCFConstants.BREAKEND_STR + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    GATKSVVCFConstants.INV33 + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    narl.leftJustifiedLeftRefLoc.getContig() + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    narl.leftJustifiedLeftRefLoc.getEnd() + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    narl.leftJustifiedRightRefLoc.getStart() + GATKSVVCFConstants.INTERVAL_VARIANT_ID_FIELD_SEPARATOR +
                    (forUpstreamLoc ? "1" : "2") ;
        }
    }
}
