package org.apache.ignite.console.jetty.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.server.AbstractNetworkConnector;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.util.MultiException;
import org.eclipse.jetty.util.resource.Resource;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.webapp.WebAppClassLoader;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.xml.XmlConfiguration;
import org.xml.sax.SAXException;

/**
 * Jetty REST protocol implementation.
 * 
 */
public class GridJettyQuercusProtocol  {
   
    static {
    	System.setProperty("org.eclipse.jetty.LEVEL", "WARN");
        System.setProperty("org.eclipse.jetty.util.log.LEVEL", "ON");
        System.setProperty("org.eclipse.jetty.util.component.LEVEL", "ON");
        
        System.setProperty("org.eclipse.jetty.server.Request.maxFormContentSize", ""+Integer.MAX_VALUE);
        System.setProperty("org.eclipse.jetty.server.Request.maxFormKeys", ""+Integer.MAX_VALUE);
        System.setProperty("LOG4J_CONFIGURATION_FILE", "config/jetty-log4j2.xml");


    }

    private static final Logger log = LogManager.getLogger(GridJettyQuercusProtocol.class);
    
    private static final String IGNITE_JETTY_HOST = "JETTY_HOST";
    private static final String IGNITE_JETTY_PORT = "JETTY_PORT";


    /** HTTP server. */
    private static Server httpSrv;

    
    private InetAddress host;
    
    private int port;

    /**
     * @param ctx Context.
     */
    public GridJettyQuercusProtocol() {
       
    }


    /** {@inheritDoc} */
    public void start() throws IgniteCheckedException {
        
        // first start instance
        if(httpSrv==null) {
        	configSingletonJetty();
     	}
        
        override(getJettyConnector());
        
        startJetty();
    }
    
    public void waitForExit() {
    	httpSrv.setStopAtShutdown(true);
    }
  
    private void override(AbstractNetworkConnector con) {
        int currPort = con.getPort();
        try {        	
        	this.port = currPort; 
            this.host = InetAddress.getByName(con.getHost());
        }
        catch (UnknownHostException e) {
           
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if Jetty started.
     */
    private boolean configSingletonJetty() throws IgniteCheckedException {
    	
    	 String jettyHost = System.getProperty(IGNITE_JETTY_HOST, "127.0.0.1");

        try {
            
        	File jettyPath = new File("config/jetty.xml");
	        final URL cfgUrl;
	         
	        if (!jettyPath.exists()) {
	            cfgUrl = null;
	
	            if (log.isDebugEnabled())
	                log.debug("Jetty configuration file is not provided, using defaults.");
	        }
	        else {
	            cfgUrl = jettyPath.toURL();
	
	            if (cfgUrl == null)
	                throw new IgniteCheckedException("Invalid Jetty configuration file: " + jettyPath,null);
	            else if (log.isDebugEnabled())
	                log.debug("Jetty configuration file: " + cfgUrl);
	        }
	
	        loadJettyConfiguration(cfgUrl);
        
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to resolve host to bind address: " + jettyHost, e);
        }

        return true;
    }
        
    /**
     * @throws IgniteCheckedException If failed.
     * @return {@code True} if Jetty started.
     */
    private boolean startJetty() throws IgniteCheckedException {
        try {
        	
            httpSrv.start();

            if (httpSrv.isStarted()) {
                for (Connector con : httpSrv.getConnectors()) {
                    int connPort = ((NetworkConnector)con).getPort();
                    log.info("Start Http Server: " + connPort);
                }

                return true;
            }

            return  false;
        }
        catch (Exception e) {
            boolean failedToBind = e instanceof SocketException;

            if (e instanceof MultiException) {
                if (log.isDebugEnabled())
                    log.debug("Caught multi exception: " + e);

                failedToBind = true;

                for (Object obj : ((MultiException)e).getThrowables())
                    if (!(obj instanceof SocketException))
                        failedToBind = false;
            }

            if (e instanceof IOException && e.getCause() instanceof SocketException)
                failedToBind = true;

            if (failedToBind) {
                if (log.isDebugEnabled())
                    log.debug("Failed to bind HTTP server to configured port.");

                stopJetty();
            }
            else
                throw new IgniteCheckedException("Failed to start Jetty HTTP server.", e);

            return false;
        }
    }

    /**
     * Loads jetty configuration from the given URL.
     *
     * @param cfgUrl URL to load configuration from.
     * @throws IgniteCheckedException if load failed.
     * @throws IOException 
     */
    private void loadJettyConfiguration(URL cfgUrl) throws IgniteCheckedException, IOException {
        if (cfgUrl == null) {        	
            HttpConfiguration httpCfg = new HttpConfiguration();

            httpCfg.setSecureScheme("https");
            httpCfg.setSecurePort(8443);
            httpCfg.setSendServerVersion(true);
            httpCfg.setSendDateHeader(true);           

            String srvPortStr = System.getProperty(IGNITE_JETTY_PORT, "8080");

            int srvPort;

            srvPort = Integer.parseInt(srvPortStr);

            httpSrv = new Server(new QueuedThreadPool(64, 4));

            ServerConnector srvConn = new ServerConnector(httpSrv, new HttpConnectionFactory(httpCfg));

            srvConn.setHost(System.getProperty(IGNITE_JETTY_HOST, "localhost"));
            srvConn.setPort(srvPort);
            srvConn.setIdleTimeout(60000L);
            srvConn.setReuseAddress(true);

            httpSrv.addConnector(srvConn);

            httpSrv.setStopAtShutdown(true);
        }
        else {
            XmlConfiguration cfg;

            try {
                cfg = new XmlConfiguration(Resource.newResource(cfgUrl));
            }
            catch (FileNotFoundException e) {
                throw new IgniteCheckedException("Failed to find configuration file: " + cfgUrl, e);
            }
            catch (SAXException e) {
                throw new IgniteCheckedException("Failed to parse configuration file: " + cfgUrl, e);
            }
            catch (IOException e) {
                throw new IgniteCheckedException("Failed to load configuration file: " + cfgUrl, e);
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to start HTTP server with configuration file: " + cfgUrl, e);
            }

            try {
                httpSrv = (Server)cfg.configure();
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to start Jetty HTTP server.", e);
            }
        }        
        
        assert httpSrv != null;
        
        //add@byron support custom rest cmd handler        
        String webAppDirs = "webapps";
		

		List<Handler> plugins = new ArrayList<>();
		File webPlugins = new File(webAppDirs);
		if(webPlugins.isDirectory()) {
					    
			for(File warFile: webPlugins.listFiles()) {
			    String warPath = warFile.getPath();
			    int pos = warFile.getName().indexOf('.');
			    String contextPath =  pos>0? warFile.getName().substring(0,pos): warFile.getName();
			    WebAppContext webApp = new WebAppContext();
			    webApp.setContextPath("/"+contextPath);
			    webApp.setConfigurationDiscovered(true);
			    
			    if (warFile.isDirectory()) {
			        // Development mode, read from FS
			    	webApp.setResourceBase(warFile.getPath());
			        webApp.setDescriptor(warPath+"/WEB-INF/web.xml");
			        webApp.setExtraClasspath(warPath+"/WEB-INF/classes/");	        
			       
		        } else if(warFile.getName().endsWith(".war")) {
			        // use packaged WAR
			        webApp.setWar(warFile.getAbsolutePath());
			        webApp.setExtractWAR(true);
			       
			    }
		        else {
		        	continue;
		        }
			    
			    ClassLoader cl = Thread.currentThread().getContextClassLoader();
			    WebAppClassLoader loader = new WebAppClassLoader(cl,webApp);			   
			    webApp.setClassLoader(loader);
			    
				webApp.setParentLoaderPriority(true);
				webApp.setServer(httpSrv);
				webApp.setErrorHandler(new ErrorHandler());		
				
				if(contextPath.equals("mongoAdmin")) {
					// 创建RewriteHandler
			        RewriteHandler rewriteHandler = new RewriteHandler();
			        rewriteHandler.setRewriteRequestURI(true);
			        rewriteHandler.setRewritePathInfo(false);
			        rewriteHandler.setOriginalPathAttribute("requestedPath");
			        

			        // 添加重写规则
			        RewriteRegexRule rewriteRule = new RewriteRegexRule();
			        rewriteRule.setRegex("^/mongoAdmin/([a-zA-Z0-9-_]*)$");
			        rewriteRule.setReplacement("/mongoAdmin/index.php?q=$1");
			        rewriteHandler.addRule(rewriteRule);

			        // 将RewriteHandler设置为处理器
			        rewriteHandler.setHandler(webApp);
			        
			        plugins.add(rewriteHandler);
				}
				
				log.info("start webapp: "+contextPath);
				plugins.add(webApp);
			
		    }
		}    
		
        
        
		// Create a handler list to store our static and servlet context handlers.
	    Handler hnd = httpSrv.getHandler();
		HandlerList handlers = new HandlerList();
		if(hnd!=null) {
			plugins.add(hnd);
			handlers.setHandlers(plugins.toArray(new Handler[plugins.size()]));	
		}
		else {
			handlers.setHandlers(plugins.toArray(new Handler[plugins.size()]));	
		}
        //-httpSrv.setHandler(jettyHnd);
        httpSrv.setHandler(handlers);
        
    }

    /**
     * Checks that the only connector configured for the current jetty instance
     * and returns it.
     *
     * @return Connector instance.
     * @throws IgniteCheckedException If no or more than one connectors found.
     */
    private AbstractNetworkConnector getJettyConnector() throws IgniteCheckedException {
        if (httpSrv.getConnectors().length == 1) {
            Connector connector = httpSrv.getConnectors()[0];

            if (!(connector instanceof AbstractNetworkConnector))
                throw new IgniteCheckedException("Error in jetty configuration. Jetty connector should extend " +
                    "AbstractNetworkConnector class." );

            return (AbstractNetworkConnector)connector;
        }
        else
            throw new IgniteCheckedException("Error in jetty configuration [connectorsFound=" +
                httpSrv.getConnectors().length + "connectorsExpected=1]");
    }

    /**
     * Stops Jetty.
     */
    private void stopJetty() {
        // Jetty does not really stop the server if port is busy.
        try {
            if (httpSrv != null) {                

                // Record current interrupted status of calling thread.
                boolean interrupted = Thread.interrupted();

                try {
                    httpSrv.stop();
                }
                finally {
                    // Reset interrupted flag on calling thread.
                    if (interrupted)
                        Thread.currentThread().interrupt();
                }
            }
        }
        catch (InterruptedException ignored) {
            if (log.isDebugEnabled())
                log.debug("Thread has been interrupted.");

            Thread.currentThread().interrupt();
        }
        catch (Exception e) {
            log.error("Failed to stop Jetty HTTP server.", e);
        }
    }

    /** {@inheritDoc} */
    public void stop() {
    	if(httpSrv!=null) {
    		stopJetty();	
	        httpSrv = null;  
    	}  
    	
    }

    
}
