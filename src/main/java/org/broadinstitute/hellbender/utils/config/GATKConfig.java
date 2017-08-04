package org.broadinstitute.hellbender.utils.config;

import org.broadinstitute.hellbender.utils.config.ConfigUtils;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Mutable;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.Sources;

import java.util.List;

/**
 * Configuration file for GATK options.
 */
@LoadPolicy(LoadType.MERGE)
@Sources({
        // This is commented out because if you dont let the ConfigFactory know about this parameter,
        // it will try to load it as text.  This results in an unhandled exception.
        // This happens when we run our tests.
        "file:${pathToMainConfig}",                                           // Variable for file loading
        "file:Main.config",                                                     // Default path
        "classpath:org/broadinstitute/hellbender/utils/config/Main.config"      // Class path
})
public interface GATKConfig extends Mutable, Accessible {

    // =================================================================================
    // General Options:
    // =================================================================================

    @DefaultValue(
            "htsjdk.variant,htsjdk.tribble,org.broadinstitute.hellbender.utils.codecs")
    List<String> codec_packages();

    // =================================================================================
    // GATKTool Options:
    // =================================================================================

    @DefaultValue("40")
    int cloudPrefetchBuffer();

    @DefaultValue("-1")
    int cloudIndexPrefetchBuffer();

    @DefaultValue("true")
    boolean createOutputBamIndex();
}