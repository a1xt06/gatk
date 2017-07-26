package org.broadinstitute.hellbender.utils.config;

import org.aeonbits.owner.Accessible;
import org.aeonbits.owner.Config;
import org.aeonbits.owner.ConfigFactory;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import static java.nio.file.StandardCopyOption.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Unit test for GATK configuration file handling.
 *
 * Created by jonn on 7/19/17.
 */
public class ConfigUnitTest {

    // ================================================================================
    // Helper Methods:
    // ================================================================================

    void listAndStoreConfigToStdOut(Accessible config) throws IOException {

        config.list(System.out);
        System.out.println();

        config.store(System.out, "");
        System.out.println();

        config.storeToXML(System.out, "");
        System.out.println();
    }

    // ================================================================================
    // Data Providers:
    // ================================================================================

    @DataProvider
    Object[][] createArgsAndConfigFileOptions() {
        return new Object[][] {
                {
                    new ArrayList<>(Arrays.asList(new String[] {"main","--zonfigurati","DUMMY_FILE","END"})),
                    "--config",
                    null,
                    new ArrayList<>(Arrays.asList(new String[] {"main","--zonfigurati","DUMMY_FILE","END"})),
                },
                {
                    new ArrayList<>(Arrays.asList(new String[] {"main","--config","DUMMY_FILE","END"})),
                    "--config",
                    "DUMMY_FILE",
                    new ArrayList<>(Arrays.asList(new String[] {"main","END"})),
                },
                {
                    new ArrayList<>(Arrays.asList(new String[] {"main","END","--config","DUMMY_FILE"})),
                    "--config",
                    "DUMMY_FILE",
                    new ArrayList<>(Arrays.asList(new String[] {"main","END"})),
                },
        };
    }

    @DataProvider
    Object[][] createArgsAndConfigFileOptionsBadInput() {
        return new Object[][] {
                {
                        new ArrayList<>(Arrays.asList(new String[] {"main", "testArg", "--config"})),
                        "--config",
                },
                {
                        new ArrayList<>(Arrays.asList(new String[] {"main","--config"})),
                        "--config",
                },
                {
                        new ArrayList<>(Arrays.asList(new String[] {"--config"})),
                        "--config",
                },
        };
    }

    // ================================================================================
    // Tests:
    // ================================================================================

    @Test(dataProvider = "createArgsAndConfigFileOptionsBadInput",
            expectedExceptions = UserException.BadInput.class)
    void testGetConfigFilenameFromArgsBadInput( final ArrayList<String> args,
                                        final String configFileOption) {

        ConfigUtils.getConfigFilenameFromArgs(args, configFileOption);
    }

    @Test(dataProvider= "createArgsAndConfigFileOptions")
    void testGetConfigFilenameFromArgs( final ArrayList<String> args,
                                        final String configFileOption,
                                        final String expectedFilename,
                                        final ArrayList<String> expectedRemainingArgs) {

        String outFileName = ConfigUtils.getConfigFilenameFromArgs(args, configFileOption);

        Assert.assertEquals(expectedFilename, outFileName);
        Assert.assertEquals(expectedRemainingArgs, args);
    }

    @Test
    void testInitializeConfiguration() throws IOException {
        String inputPropertiesFile = "src/test/resources/org/broadinstitute/hellbender/utils/config/AdditionalTestOverrides.properties";

        BasicTestConfig basicTestConfig = ConfigUtils.initializeConfiguration(inputPropertiesFile, BasicTestConfig.class);

        // List properties for inspection:
        listAndStoreConfigToStdOut(basicTestConfig);

        // Check for our values:
        Assert.assertEquals(basicTestConfig.booleanDefFalse(), false);
        Assert.assertEquals(basicTestConfig.booleanDefTrue(), true);
        Assert.assertEquals(basicTestConfig.intDef207(), 999);
        Assert.assertEquals(basicTestConfig.listOfStringTest(), new ArrayList<>(Arrays.asList(new String[] {"string1", "string2", "string3", "string4"})));

    }

    @Test
    void testOwnerConfiguration() throws IOException {

        // Test with our basic test class:
        BasicTestConfig basicTestConfig = ConfigFactory.create(BasicTestConfig.class);

        // List properties for inspection:
        listAndStoreConfigToStdOut(basicTestConfig);

        Assert.assertEquals(basicTestConfig.booleanDefFalse(), false);
        Assert.assertEquals(basicTestConfig.booleanDefTrue(), true);
        Assert.assertEquals(basicTestConfig.intDef207(), 207);
        Assert.assertEquals(basicTestConfig.listOfStringTest(), new ArrayList<>(Arrays.asList(new String[] {"string1", "string2", "string3", "string4"})));

    }

    @Test
    void testOwnerConfigurationWithClassPathOverrides() throws IOException {

        // Test with the class that overrides on the class path:
        BasicTestConfigWithClassPathOverrides basicTestConfigWithClassPathOverrides =
                ConfigFactory.create(BasicTestConfigWithClassPathOverrides.class);

        // List properties for inspection:
        listAndStoreConfigToStdOut(basicTestConfigWithClassPathOverrides);

        Assert.assertEquals(basicTestConfigWithClassPathOverrides.booleanDefFalse(), true);
        Assert.assertEquals(basicTestConfigWithClassPathOverrides.booleanDefTrue(), false);
        Assert.assertEquals(basicTestConfigWithClassPathOverrides.intDef207(), 702);
        Assert.assertEquals(basicTestConfigWithClassPathOverrides.listOfStringTest(), new ArrayList<>(Arrays.asList(new String[] {"string4", "string3", "string2", "string1"})));
    }

    @Test
    void testOwnerConfigurationWithClassPathOverridesAndVariableFileInput() throws IOException {

        // Start with the name of the properties file to copy:
        String overrideFilename = "AdditionalTestOverrides.properties";

        // Create a temporary folder in which to place the config file:
        final File outputDir = Files.createTempDirectory("testOwnerConfigurationWithClassPathOverridesAndVariableFileInput").toAbsolutePath().toFile();
        outputDir.deleteOnExit();

        // Put the known config file in the new directory:
        Files.copy(new File("src/test/resources/org/broadinstitute/hellbender/utils/config/" + overrideFilename).toPath(),
                new File(outputDir.getAbsolutePath() + File.separator + overrideFilename).toPath(),
                REPLACE_EXISTING);

        // Set our file location here:
        ConfigFactory.setProperty("pathToConfigFile", outputDir.getAbsolutePath() + File.separator + overrideFilename);

        // Test with the class that overrides on the class path:
        BasicTestConfigWithClassPathOverridesAndVariableFile basicTestConfigWithClassPathOverridesAndVariableFile =
                ConfigFactory.create(BasicTestConfigWithClassPathOverridesAndVariableFile.class);

        // List properties for inspection:
        listAndStoreConfigToStdOut(basicTestConfigWithClassPathOverridesAndVariableFile);

        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.booleanDefFalse(), true);
        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.booleanDefTrue(), false);
        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.intDef207(), 999);
        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.listOfStringTest(), new ArrayList<>(Arrays.asList(new String[] {"string4", "string3", "string2", "string1"})));
    }

    @Test
    void testOwnerConfigurationWithClassPathOverridesAndVariableFileInput_NoGivenFile() throws IOException {

        // Start with the name of the properties file to copy:
        String overrideFilename = "AdditionalTestOverrides.properties";

        // Create a temporary folder in which to place the config file:
        final File outputDir = Files.createTempDirectory("testOwnerConfigurationWithClassPathOverridesAndVariableFileInput").toAbsolutePath().toFile();
        outputDir.deleteOnExit();

        // Set our file location here:
        ConfigFactory.setProperty("pathToConfigFile", outputDir.getAbsolutePath() + File.separator + overrideFilename);

        // Test with the class that overrides on the class path:
        BasicTestConfigWithClassPathOverridesAndVariableFile basicTestConfigWithClassPathOverridesAndVariableFile =
                ConfigFactory.create(BasicTestConfigWithClassPathOverridesAndVariableFile.class);

        // List properties for inspection:
        listAndStoreConfigToStdOut(basicTestConfigWithClassPathOverridesAndVariableFile);

        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.booleanDefFalse(), true);
        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.booleanDefTrue(), false);
        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.intDef207(), 702);
        Assert.assertEquals(basicTestConfigWithClassPathOverridesAndVariableFile.listOfStringTest(), new ArrayList<>(Arrays.asList(new String[] {"string4", "string3", "string2", "string1"})));
    }
}
