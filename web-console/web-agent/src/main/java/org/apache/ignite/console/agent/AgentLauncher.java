

package org.apache.ignite.console.agent;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;

import org.apache.ignite.console.agent.handlers.WebSocketRouter;
import org.apache.ignite.internal.util.nodestart.IgniteRemoteStartSpecification;
import org.apache.ignite.internal.util.nodestart.IgniteSshHelper;
import org.apache.ignite.internal.util.nodestart.StartNodeCallable;
import org.apache.ignite.internal.util.typedef.F;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * Ignite Web Console Agent launcher.
 */
public class AgentLauncher implements IgniteSshHelper {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AgentLauncher.class);

    static {
        // Optionally remove existing handlers attached to j.u.l root logger.
        // -SLF4JBridgeHandler.removeHandlersForRootLogger();

        // Add SLF4JBridgeHandler to j.u.l's root logger.
        // -SLF4JBridgeHandler.install();
    }

    /**
     * @param fmt Format string.
     * @param args Arguments.
     */
    private static char[] readPassword(String fmt, Object... args) {
        if (System.console() != null)
            return System.console().readPassword(fmt, args);

        System.out.print(String.format(fmt, args));

        return new Scanner(System.in).nextLine().toCharArray();
    }

    /**
     * @param args Launcher args.
     * @return Agent configuration;
     */
     static AgentConfiguration parseArgs(String[] args) {
        log.info("Starting Apache Ignite Web Console Agent...");

        AgentConfiguration cfg = new AgentConfiguration();

        JCommander jCommander = new JCommander(cfg);

        String osName = System.getProperty("os.name").toLowerCase();

        jCommander.setProgramName("ignite-console-web-agent." + (osName.contains("win") ? "bat" : "sh"));

        try {
            jCommander.parse(args);
        }
        catch (ParameterException pe) {
            log.error("Failed to parse command line parameters: " + Arrays.toString(args), pe);

            jCommander.usage();

            return null;
        }

        String prop = cfg.configPath();

        AgentConfiguration propCfg = new AgentConfiguration();

        try {
            File f = AgentUtils.resolvePath(prop);

            if (f == null)
                log.warn("Failed to find properties file: " + prop);
            else
                propCfg.load(f.toURI().toURL());
        }
        catch (IOException e) {
            if (!AgentConfiguration.DFLT_CFG_PATH.equals(prop))
                log.warn("Failed to load properties file: " + prop, e);
        }

        cfg.merge(propCfg);

        if (cfg.help()) {
            jCommander.usage();

            return null;
        }

        System.out.println();
        System.out.println("Web Console Agent configuration :");
        System.out.println(cfg);
        System.out.println();

        URI uri;

        try {
            uri = URI.create(cfg.serverUri());

            if (uri.getScheme().startsWith("http")) {
                uri = new URI("http".equalsIgnoreCase(uri.getScheme()) ? "ws" : "wss",
                    uri.getUserInfo(),
                    uri.getHost(),
                    uri.getPort(),
                    uri.getPath(),
                    uri.getQuery(),
                    uri.getFragment()
                );
                
                cfg.serverUri(uri.toString());
            }
        }
        catch (Exception e) {
            log.error("Failed to parse Ignite Web Console uri", e);

            return null;
        }

        if (cfg.tokens() == null) {
            System.out.println("Security token is required to establish connection to the Web Console.");
            System.out.println(String.format("It is available on the Profile page: https://%s/profile", uri.getHost()));

            String tokens = String.valueOf(readPassword("Enter security tokens separated by comma: "));

            cfg.tokens(new ArrayList<>(Arrays.asList(tokens.trim().split(","))));
        }

        if (!F.isEmpty(cfg.passwordsStore()) && F.isEmpty(cfg.passwordsStorePassword())) {
            System.out.println("The passwords key store path is specified, but password is empty!");

            String pwd = String.valueOf(readPassword("Enter password for key store: "));

            cfg.passwordsStorePassword(pwd);
        }

        List<String> nodeURIs = cfg.nodeURIs();

        for (int i = nodeURIs.size() - 1; i >= 0; i--) {
            String nodeURI = nodeURIs.get(i);

            try {
                new URI(nodeURI);
            }
            catch (URISyntaxException ignored) {
                log.warn("Failed to parse Ignite node URI: " + nodeURI);

                nodeURIs.remove(i);
            }
        }

        if (nodeURIs.isEmpty()) {
            log.error("Failed to find valid URIs for connect to Ignite node via REST. Please check settings");

            return null;
        }

        cfg.nodeURIs(nodeURIs);

        return cfg;
    }
     
     /** {@inheritDoc} */
     @Override 
     public StartNodeCallable nodeStartCallable(IgniteRemoteStartSpecification spec, int timeout) {     	
    	// use agent server
  		return new IgniteClusterLauncher(spec,timeout);
     }

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        AgentConfiguration cfg = parseArgs(args);

        // Failed to parse configuration or help printed.
        if (cfg == null)
            return;
        
        WebSocketRouter websocket = new WebSocketRouter(cfg);

        while(true) {
	        try {
	        	
	            websocket.start();
	
	            websocket.awaitClose();
	            
	            Thread.sleep(1000);
	        }
	        catch (Throwable ignored) {
	            // No-op.
	        	ignored.printStackTrace();
	        	break;
	        }
        }
        websocket.close();
    }
}
