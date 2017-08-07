package org.broadinstitute.hellbender.utils.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Mutable;
import org.aeonbits.owner.Config.LoadPolicy;
import org.aeonbits.owner.Config.LoadType;
import org.aeonbits.owner.Config.Sources;

/**
 * Configuration file for System-level options.
 * All options in this class are intended to be injected into the JRE as System Properties.
 */
@LoadPolicy(LoadType.MERGE)
@Sources({ "file:SystemProperties.config",                                                     // Default path
        "classpath:org/broadinstitute/hellbender/utils/config/SystemProperties.config" })   // Class path
public interface SystemPropertiesConfig extends Mutable, Accessible {

    // =================================================================================
    // General Options:
    // =================================================================================

    @DefaultValue("true")
    boolean GATK_STACKTRACE_ON_USER_EXCEPTION();

    // =================================================================================
    // SAMJDK Options:
    // =================================================================================

    @Key("samjdk.use_async_io_read_samtools")
    @ConverterClass(CustomBooleanConverter.class)
    @DefaultValue("false")
    Boolean samjdk_use_async_io_read_samtools();

    @Key("samjdk.use_async_io_write_samtools")
    @DefaultValue("true")
    boolean samjdk_use_async_io_write_samtools();

    @Key("samjdk.use_async_io_write_tribble")
    @DefaultValue("false")
    boolean samjdk_use_async_io_write_tribble();

    @Key("samjdk.compression_level")
    @DefaultValue("1")
    int samjdk_compression_level();

    // =================================================================================
    // Spark Options:
    // =================================================================================

    @Key("spark.kryoserializer.buffer.max")
    @DefaultValue("512m")
    String spark_kryoserializer_buffer_max();

    @Key("spark.driver.maxResultSize")
    @DefaultValue("0")
    int spark_driver_maxResultSize();

    @Key("spark.driver.userClassPathFirst")
    @DefaultValue("true")
    boolean spark_driver_userClassPathFirst();

    @Key("spark.io.compression.codec")
    @DefaultValue("lzf")
    String spark_io_compression_codec();

    @Key("spark.yarn.executor.memoryOverhead")
    @DefaultValue("600")
    int spark_yarn_executor_memoryOverhead();

    @Key("spark.driver.extraJavaOptions")
    @DefaultValue("")
    String spark_driver_extraJavaOptions();

    @Key("spark.executor.extraJavaOptions")
    @DefaultValue("")
    String spark_executor_extraJavaOptions();

    // =================================================================================
    // Other Options:
    // =================================================================================

    @Key("snappy.disable")
    @DefaultValue("true")
    boolean snappy_disable();
}