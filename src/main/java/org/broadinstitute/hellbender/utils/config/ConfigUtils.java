package org.broadinstitute.hellbender.utils.config;

import org.aeonbits.owner.*;
import org.broadinstitute.hellbender.exceptions.UserException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class to contain configuration utility methods.
 * Created by jonn on 7/19/17.
 */
public class ConfigUtils {

    // This class just has static methods to help with configuration, so no need for this constructor.
    private ConfigUtils() {}

    /**
     * Whether we have already set the config factory variable defaults.
     */
    static boolean hasSetConfigFactoryVariableDefaults = false;

    /**
     * Sets the {@link org.aeonbits.owner.ConfigFactory} variables so that it knows about
     * the variable paths for config files.
     */
    public static final void setConfigFactoryVariableDefaults() {

        // You only need to do this once.

        if ( !hasSetConfigFactoryVariableDefaults ) {

            ArrayList<String> propertyNames = new ArrayList<>();

            // Get the classes from which we need to look for sources:
            Class<?>[] configurationClasses = new Class<?>[] {
                    SystemPropertiesConfig.class,
                    GATKConfig.class
            };

            // Create a regex to use to look for variables in the Sources annotation:
            Pattern p = Pattern.compile("\\$\\{(.*)}");

            // Loop through our classes and grab any sources with variables in there:
            for ( Class<?> clazz : configurationClasses ) {
                Set<Class<?>> interfaces = new HashSet<>(Arrays.asList(clazz.getInterfaces()));

                // Make sure that we get config classes here:
                if ( interfaces.contains(Config.class) ||
                         interfaces.contains(Accessible.class) ||
                         interfaces.contains(Mutable.class) ||
                         interfaces.contains(Reloadable.class) ) {
                    Config.Sources annotation = clazz.getAnnotation(Config.Sources.class);

                    String[] annotationValues = annotation.value();

                    for ( String val : annotationValues ) {

                        Matcher m = p.matcher(val);
                        if ( m.find() ) {
                            propertyNames.add(m.group(1));
                        }
                    }
                }
            }

//            // The list of properties we need to make sure are defined
//            // either in system properties or environment properties:
//            String[] propertyNames = new String[]{
//                    "pathToMainConfig",
//            };

            // Grab the system properties:
            Properties systemProperties = System.getProperties();

            // Grab the environment properties:
            Map<String, String> environmentProperties = System.getenv();

            // Make sure that if our property isn't in the system and environment
            // properties, that we set it to a neutral value that will not contain
            // anything (so that the property will fall back into the next value).
            for (String property : propertyNames) {

                if ((!environmentProperties.keySet().contains(property)) &&
                        (!systemProperties.containsKey(property))) {

                    ConfigFactory.setProperty(property, "/dev/null");
                }
            }

            hasSetConfigFactoryVariableDefaults = true;
        }
    }

    /**
     * Get the configuration file name from the given arguments.
     * Modifies the given arguments to remove both the configuration file specification string
     * and the configuration file name from the args.
     *
     * NOTE: Does NOT validate that the resulting string is a configuration file.
     *
     * @param args Command-line arguments passed to this program.
     * @param configFileOption The command-line option indicating that the config file is next
     * @return The name of the configuration file for this program or {@code null}.
     */
    public static final String getConfigFilenameFromArgs( final ArrayList<String> args, final String configFileOption ) {

        String configFileName = null;

        for ( int i = 0 ; i < args.size() ; ++i ) {
            if (args.get(i).compareTo(configFileOption) == 0) {

                // Get rid of the command-line argument name:
                args.remove(i);

                if ( i < args.size() ) {

                    // Get and remove the specified config file:
                    configFileName = args.remove(i);
                    break;
                }
                else {
                    // Option was provided, but no file was specified.
                    // We cannot work under these conditions:

                    String message = "ERROR: Configuration file not given after config file option specified: " + configFileOption;
                    System.err.println(message);
                    throw new UserException.BadInput(message);
                }
            }
        }

        return configFileName;
    }

    /**
     * Get the configuration filenames the command-line (if they exist) and create configurations for them.
     * Removes the configuration filenames and configuration file options from the given {@code argList}.
     * Also sets system-level properties from the system config file.
     * @param argList The list of arguments from which to read the config file.
     * @param mainConfigFileOption The command-line option specifying the main configuration file.
     * @param systemPropertiesConfigurationFileOption The command-line option specifying the system properties configuration file.
     */
    public static final void initializeConfigurationsFromCommandLineArgs(final ArrayList<String> argList,
                                                                         String mainConfigFileOption,
                                                                         String systemPropertiesConfigurationFileOption) {
        // Get main config from args:
        final String mainConfigFileName = getConfigFilenameFromArgs( argList, mainConfigFileOption );

        // Get system properties config from args:
        final String systemConfigFileName = getConfigFilenameFromArgs( argList, systemPropertiesConfigurationFileOption );

        // Alternate way to load the config file:
        GATKConfig gatkConfig = ConfigUtils.initializeConfiguration( systemConfigFileName, GATKConfig.class );

        // NOTE: Alternate way to load the config file:
        SystemPropertiesConfig systemPropertiesConfig = ConfigUtils.initializeConfiguration( systemConfigFileName, SystemPropertiesConfig.class );

        // To start with we inject our system properties to ensure they are defined for downstream components:
        ConfigUtils.injectSystemPropertiesFromSystemConfig( systemPropertiesConfig );
    }

    /**
     * Initializes and returns the configuration as specified by {@code configFileName}
     * Also caches this configuration in the {@link ConfigCache} for use elsewhere.
     * @param configFileName The name of the file from which to initialize the configuration
     * @param configClass The type of configuration in which to interpret the given {@code configFileName}
     * @return The configuration instance implementing {@link GATKConfig} containing any overrides in the given file.
     */
    public static final <T extends Config> T initializeConfiguration(final String configFileName, Class<? extends T> configClass) {

        // Get a place to store our properties:
        final Properties userConfigFileProperties = new Properties();

        // Try to get the config from the specified file:
        if ( configFileName != null ) {

            try {
                final FileInputStream userConfigFileInputStream = new FileInputStream(configFileName);
                userConfigFileProperties.load(userConfigFileInputStream);

                if (configFileName != null) {
                    System.out.println("Found " + configClass.getSimpleName() + " Configuration File: " + configFileName);
                }

            } catch (final FileNotFoundException e) {
                System.err.println("WARNING: unable to find specified " + configClass.getSimpleName() + " configuration file: "
                        + configFileName + " - defaulting to built-in config settings.");
            }
            catch (final IOException e) {
                System.err.println("WARNING: unable to load specified " + configClass.getSimpleName() + " configuration file: "
                        + configFileName + " - defaulting to built-in config settings.");
            }
        }

        // Cache and return our configuration:
        // NOTE: The configuration will be stored in the ConfigCache under the key GATKConfig.class.
        //       This means that any future call to getOrCreate for this GATKConfig.class will return
        //       Not only the configuration itself, but also the overrides as specified in userConfigFileProperties
        return ConfigCache.getOrCreate(configClass, userConfigFileProperties);
    }

    /**
     * Injects system properties from the given configuration file.
     * @param config The {@link GATKConfig} object from which to inject system properties.
     */
    public static final void injectSystemPropertiesFromSystemConfig(SystemPropertiesConfig config) {

        // Set all system properties in our config:

        System.setProperty(
                "GATK_STACKTRACE_ON_USER_EXCEPTION",
                Boolean.toString( config.GATK_STACKTRACE_ON_USER_EXCEPTION() )
        );

        System.setProperty(
                "samjdk.use_async_io_read_samtools",
                Boolean.toString(config.samjdk_use_async_io_read_samtools())
        );

        System.setProperty(
                "samjdk.use_async_io_write_samtools",
                Boolean.toString(config.samjdk_use_async_io_write_samtools())
        );

        System.setProperty(
                "samjdk.use_async_io_write_tribble",
                Boolean.toString(config.samjdk_use_async_io_write_tribble())
        );

        System.setProperty(
                "samjdk.compression_level",
                Integer.toString(config.samjdk_compression_level() )
        );

        System.setProperty(
                "snappy.disable",
                Boolean.toString(config.snappy_disable())
        );
    }
}
