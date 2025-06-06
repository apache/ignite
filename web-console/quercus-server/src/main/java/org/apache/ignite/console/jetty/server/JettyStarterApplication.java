package org.apache.ignite.console.jetty.server;


/**
 * Web console launcher.
 */


public class JettyStarterApplication{
    /**
     * @param args Args.
     */
    public static void main(String[] args) {

        String quercusWebappDir = "";
        if(args.length>0){
            quercusWebappDir = args[0];
        }
        if (!quercusWebappDir.isBlank()) {
            System.setProperty("quercus.webapp_dir", quercusWebappDir);
        }
    	GridJettyQuercusProtocol jettyEmbed = new GridJettyQuercusProtocol();
    	jettyEmbed.start();
    	jettyEmbed.waitForExit();
    }
}
