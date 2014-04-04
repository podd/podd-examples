/**
 * 
 */
package podd.test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import joptsimple.OptionException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import podd.ExampleCLI;

/**
 * Test for the HRPPC TrayScan CLI.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class ExampleCLITest
{
    @Rule
    public TemporaryFolder tempDir = new TemporaryFolder();
    
    private static final String EOL = System.getProperty("line.separator");
    private PrintStream console;
    private ByteArrayOutputStream bytes;
    
    private Path testDir;
    
    private String oldHome;
    
    @Before
    public void setUp() throws Exception
    {
        this.bytes = new ByteArrayOutputStream();
        this.console = System.out;
        System.setOut(new PrintStream(this.bytes));
        
        this.testDir = this.tempDir.newFolder("hrppc-trayscan-client-test").toPath();
        
        System.out.println("user.home=" + System.getProperty("user.home"));
        System.out.println("user.dir=" + System.getProperty("user.dir"));
        
        this.oldHome = System.setProperty("user.home", this.testDir.toAbsolutePath().toString());
        final Path propertiesFile = Paths.get(this.oldHome, ".podd/poddclient.properties");
        Assert.assertTrue("Must configure properties file in ~/.podd/poddclient.properties: " + propertiesFile,
                Files.exists(propertiesFile));
        Files.copy(Paths.get(this.oldHome, ".podd/poddclient.properties"),
                this.testDir.resolve("poddclient.properties"));
    }
    
    @After
    public void tearDown() throws Exception
    {
        System.setProperty("user.home", this.oldHome);
        System.setOut(this.console);
    }
    
    @Test
    public void testNoArgsHelp() throws Exception
    {
        ExampleCLI.main();
        this.assertHelp(true);
    }
    
    @Test
    public void testExplicitHelp() throws Exception
    {
        ExampleCLI.main("--help");
        this.assertHelp(true);
    }
    
    @Test
    public void testVerboseEmpty() throws Exception
    {
        ExampleCLI.main("--verbose");
        this.assertHelp(true);
    }
    
    @Test
    public void testVerboseUnknown() throws Exception
    {
        try
        {
            ExampleCLI.main("--non-existent", "--verbose");
            Assert.fail("Did not receive expected exception");
        }
        catch(final OptionException e)
        {
            Assert.assertTrue(e.getMessage().contains("'non-existent' is not a recognized option"));
        }
        finally
        {
            this.assertHelp(true);
        }
    }
    
    @Test
    public void testNonVerboseUnknown() throws Exception
    {
        try
        {
            ExampleCLI.main("--non-existent", "--verbose", "false");
            Assert.fail("Did not receive expected exception");
        }
        catch(final OptionException e)
        {
            Assert.assertTrue(e.getMessage().contains("'non-existent' is not a recognized option"));
        }
        finally
        {
            this.assertHelp(true);
        }
    }
    
    @Test
    public void testExperimentBarcodeNoOutputDir() throws Exception
    {
        try
        {
            ExampleCLI.main("--experiment-barcode", "Project#2013-0015_Experiment#0001");
            Assert.fail("Did not receive expected exception");
        }
        catch(final OptionException e)
        {
            Assert.assertTrue(e.getMessage().contains("Missing required option(s) ['output-dir']"));
        }
        finally
        {
            this.assertHelp(true);
        }
    }
    
    @Test
    public void testExperimentBarcodeWithOutputDir() throws Exception
    {
        final Path dumpDir = Paths.get("/home/ans025/temp/trayscandump-2c8e4a78-821f-41af-9678-658548f0a47a");
        final String projectAndExperimentBarcode = "Project#2013-0015_Experiment#0001";
        
        try
        {
            ExampleCLI.main("--experiment-barcode", projectAndExperimentBarcode, "--output-dir",
                    dumpDir.toString());
        }
        finally
        {
            this.assertHelp(false);
        }
    }
    
    /**
     * Assert that the help messages were present in the output.
     */
    private void assertHelp(final boolean shouldShow) throws Exception
    {
        final String results = this.bytes.toString();
        this.console.print(results);
        Assert.assertEquals(shouldShow, results.contains("Option"));
        Assert.assertEquals(shouldShow, results.contains("Description"));
        Assert.assertEquals(shouldShow, results.contains("--dump-bags"));
        Assert.assertEquals(shouldShow, results.contains("--experiment-barcode"));
        Assert.assertEquals(shouldShow, results.contains("--help"));
        Assert.assertEquals(shouldShow, results.contains("--output-dir"));
        Assert.assertEquals(shouldShow, results.contains("--verbose"));
    }
    
}
