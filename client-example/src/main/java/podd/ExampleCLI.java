/**
 * 
 */
package podd;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import net.schmizz.sshj.userauth.password.PasswordFinder;

import com.github.ansell.propertyutil.PropertyUtil;
import com.github.podd.example.ExamplePoddClient;

/**
 * The main HRPPC PODD Command Line Interface file.
 * 
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class ExampleCLI
{
    public static void main(final String... args) throws Exception
    {
        System.out.println("user.home=" + System.getProperty("user.home"));
        System.out.println("user.dir=" + System.getProperty("user.dir"));
        
        // Scan the loaded properties, which can be overriden using system properties (ie,
        // -Dverbose=... on command line)
        final OptionParser parser = new OptionParser();
        
        final OptionSpec<Void> help = parser.accepts("help").forHelp();
        final OptionSpec<Boolean> verbose =
                parser.accepts("verbose").withOptionalArg().ofType(Boolean.class).defaultsTo(Boolean.FALSE);
        final OptionSpec<String> experimentBarcode =
                parser.accepts("experiment-barcode").withRequiredArg().ofType(String.class)
                        .describedAs("Barcode for experiment to be dumped. E.g. Project#2013-0015_Experiment#0001");
        final OptionSpec<File> outputDir =
                parser.accepts("output-dir").requiredIf(experimentBarcode).withRequiredArg().ofType(File.class)
                        .describedAs("Output directory for TrayScan images and bags");
        final OptionSpec<Boolean> dumpBags =
                parser.accepts("dump-bags").withRequiredArg().ofType(Boolean.class).defaultsTo(Boolean.TRUE)
                        .describedAs("Whether to dump bags");
        final OptionSpec<String> poddClientPropertiesFile =
                parser.accepts("podd-client-properties").withRequiredArg().ofType(String.class)
                        .describedAs("PODD Client properties file (e.g., 'poddclient' to use poddclient.properties)");
        
        OptionSet options = null;
        
        try
        {
            options = parser.parse(args);
        }
        catch(final OptionException e)
        {
            System.out.println(e.getMessage());
            // If exception thrown then options will be null, so need to manually check for verbose
            // in this case
            final List<String> rawList = Arrays.asList(args);
            parser.printHelpOn(System.out);
            throw e;
        }
        
        String propertiesFileName = "poddclient";
        
        if(options.has(poddClientPropertiesFile))
        {
            propertiesFileName = poddClientPropertiesFile.value(options);
        }
        
        System.out.println("Properties file name: " + propertiesFileName);
        final PropertyUtil props = new PropertyUtil(propertiesFileName);
        
        final ExamplePoddClient client = new ExamplePoddClient();
        client.setProps(props);
        
        if(options.has(help))
        {
            parser.printHelpOn(System.out);
        }
        else if(options.has(experimentBarcode))
        {
            if(options.has(outputDir))
            {
                final Path outputDirPath = outputDir.value(options).toPath();
                if(!Files.exists(outputDirPath))
                {
                    Files.createDirectories(outputDirPath);
                }
                final Map<String, Path> dumpTrayscanExperiment =
                        client.dumpTrayscanExperimentToBagIt(experimentBarcode.value(options), outputDirPath,
                                dumpBags.value(options));
                
                if(!dumpTrayscanExperiment.isEmpty())
                {
                    final char[] passphrase =
                            System.console().readPassword("[%s]", "Enter passphrase to unlock private key: ");
                    
                    final PasswordFinder keyExtractor = new PasswordFinder()
                        {
                            @Override
                            public boolean shouldRetry(final net.schmizz.sshj.userauth.password.Resource<?> resource)
                            {
                                return false;
                            }
                            
                            @Override
                            public char[] reqPassword(final net.schmizz.sshj.userauth.password.Resource<?> resource)
                            {
                                return passphrase;
                            }
                        };
                    
                    for(final Entry<String, Path> nextEntry : dumpTrayscanExperiment.entrySet())
                    {
                        System.out.println("Dumped: " + nextEntry.getKey() + " => " + nextEntry.getValue());
                        if(nextEntry.getKey().startsWith("bag"))
                        {
                            client.uploadToCherax(Arrays.asList(nextEntry.getValue()), outputDirPath, keyExtractor);
                        }
                    }
                    // Clear out passphrase from memory at this point
                    Arrays.fill(passphrase, ' ');
                }
            }
            else
            {
                System.err.println("No output-dir specified for experiment dump");
            }
        }
        else
        {
            parser.printHelpOn(System.out);
        }
    }
}
