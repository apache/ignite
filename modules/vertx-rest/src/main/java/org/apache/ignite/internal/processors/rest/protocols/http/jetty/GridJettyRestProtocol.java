/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.rest.protocols.http.jetty;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_HOST;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_JETTY_PORT;
import static org.apache.ignite.spi.IgnitePortProtocol.TCP;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.plugin.IgniteVertxPlugin;
import org.apache.ignite.internal.processors.rest.GridRestProtocolHandler;
import org.apache.ignite.internal.processors.rest.protocols.GridRestProtocolAdapter;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.IgniteSpiException;

import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.type.filter.AnnotationTypeFilter;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.webmvc.annotation.VertxletMapping;
import io.vertx.webmvc.creater.WebApiCreater;
import io.vertx.webmvc.starter.VertXStarter;

/**
 * Vertx REST protocol implementation.
 * 
 */
public class GridJettyRestProtocol extends GridRestProtocolAdapter {
   
	private final Properties props = new Properties();

    private GridCmdRestHandler jettyHnd;
    
    private GridServiceRestHandler serviceHnd;

    /** HTTP server. */
    private WebApiCreater httpSrv;
    
    private ServerSocket serverSocketPlaceholder;
    
    private static int handlerCount = 0;

    /**
     * @param ctx Context.
     */
    public GridJettyRestProtocol(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return "Vertx REST for Ignite Instance "+ ctx.igniteInstanceName();
    }

    /** {@inheritDoc} */
    @Override public void start(GridRestProtocolHandler hnd) throws IgniteCheckedException {
        assert ctx.config().getConnectorConfiguration() != null;        

        String jettyHost = System.getProperty(IGNITE_JETTY_HOST, ctx.config().getLocalHost());

        try {
            System.setProperty(IGNITE_JETTY_HOST, U.resolveLocalHost(jettyHost).getHostAddress());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to resolve host to bind address: " + jettyHost, e);
        }

        jettyHnd = new GridCmdRestHandler(hnd, new C1<String, Boolean>() {
            @Override public Boolean apply(String tok) {
                return F.isEmpty(secretKey) || authenticate(tok);
            }
        }, ctx); 
        
        
        serviceHnd = new GridServiceRestHandler(hnd, new C1<String, Boolean>() {
            @Override public Boolean apply(String tok) {
                return F.isEmpty(secretKey) || authenticate(tok);
            }
        }, ctx); 
        
        
        // first start instance
        if(httpSrv==null) {
        	configSingletonJetty();
        	handlerCount = 0;      	
     	}
        else {
        	handlerCount ++;
        }
    }
    
    public static boolean isPortInUse(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            return false;
        } catch (IOException e) {
            return true;
        }
    }
    
    /** {@inheritDoc} */
    @Override public void onKernalStart() {
    	
    	if(httpSrv.isClosed()) {
    		
    		final IgniteEx ignite = ctx.grid();
        		
        	if(true) {
        		
        		httpSrv.setReadyCallback((router)->{
        			try {
        				serverSocketPlaceholder.close();
        			} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
        		});
        	}        	
    		
    		ctx.pools().getRestExecutorService().submit(()->{
    			
            	try {
    	        	int waitCount = 0;
    	    		while(!ignite.cluster().active() && waitCount<60*10) {
    	    			waitCount++;
    	    			
    					Thread.sleep(1000);
    					
    	    			if(waitCount%60==0) {
    	    				log.info("Wait for cluster {} active...",ignite.name());
    	    			}					
    	    		}
    	    		
    	    		Vertx vertx;
    	    		if(!ignite.cluster().active()) {
    	        		log.info("[Vertx web] Vertx started in standard mode.");
    	        		vertx = Vertx.vertx();
    	        	}
    	        	else {
    	        		log.info("[Vertx web] Vertx started in cluster mode.");
    	        		IgniteVertxPlugin plugin = ignite.plugin("Vertx");
    	        		vertx = plugin.vertx();
    	        	}
    	    		
    	    		Future<String> rv = vertx.deployVerticle(httpSrv);
    	        	while(!rv.isComplete()) {
    	        		Thread.sleep(200);
    	        	}
    	        	
    	        	if(!httpSrv.isStarted()) {
    	            	log.info("Failed to start Vertx REST server (possibly all ports in range are in use) ");
    	                
    	                return;
    	            }
    	        	
    	        	httpSrv.getRouter().route("/ignite*").blockingHandler(jettyHnd);    	    		
    	    		
    	    		httpSrv.getRouter().get("/service/*").blockingHandler(serviceHnd);
    	    		httpSrv.getRouter().post("/service/*").blockingHandler(serviceHnd);
    	    		
    	    		httpSrv.getRouter().get("/*").last().handler(new GridStaticRestHandler(ctx));
    	        	
            	} catch (InterruptedException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    		});
    		
    		ctx.ports().registerPort(port, TCP, getClass());
			
     	}
    	
    }   
    
    private void override(HttpServerOptions con) {
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
     * @return {@code True} if Vertx started.
     */
    private boolean configSingletonJetty() throws IgniteCheckedException {
    	
    	String jettyHost = System.getProperty(IGNITE_JETTY_HOST, ctx.config().getLocalHost());

        try {
            System.setProperty(IGNITE_JETTY_HOST, U.resolveLocalHost(jettyHost).getHostAddress());
            String jettyPath = config().getJettyPath();
	        final URL cfgUrl;
	
	        if (jettyPath == null) {
	            cfgUrl = null;
	
	            if (log.isDebugEnabled())
	                log.debug("Vertx configuration file is not provided, using defaults.");
	        }
	        else {
	            cfgUrl = U.resolveIgniteUrl(jettyPath);
	
	            if (cfgUrl == null)
	                throw new IgniteSpiException("Invalid Vertx configuration file: " + jettyPath);
	            else if (log.isDebugEnabled())
	                log.debug("Vertx configuration file: " + cfgUrl);
	        }
	
	        loadJettyConfiguration(cfgUrl);
        
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to resolve host to bind address: " + jettyHost, e);
        }

        return true;
    }
        
    
    
    public void scanClasses(String basePackage) {
        ClassPathScanningCandidateComponentProvider scanner = 
            new ClassPathScanningCandidateComponentProvider(true); // 使用默认过滤器
        
        // 可以添加自定义过滤器，例如扫描特定注解的类
        scanner.addIncludeFilter(new AnnotationTypeFilter(VertxletMapping.class));
        
        for (BeanDefinition bd : scanner.findCandidateComponents(basePackage)) {
            String className = bd.getBeanClassName();
            System.out.println("Found class: " + className);
            
            try {
                Class<?> clazz = Class.forName(className);
                // 对找到的类进行处理...
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Loads Vertx configuration from the given URL.
     *
     * @param cfgUrl URL to load configuration from.
     * @throws IgniteCheckedException if load failed.
     * @throws IOException 
     */
    private void loadJettyConfiguration(@Nullable URL cfgUrl) throws IgniteCheckedException, IOException {
    	// 创建应用上下文并指定要扫描的包
    	AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    	context.scan("io.vertx.webmvc","org.apache.ignite.internal.processors.rest.igfs","org.elasticsearch.relay"); // 指定要扫描的包 
  
    	context.refresh();
    	
    	context.start();
    	
        if (cfgUrl == null) {        	
        	HttpServerOptions srvConn = new HttpServerOptions();

            String srvPortStr = System.getProperty(IGNITE_JETTY_PORT, ""+8080);

            int srvPort;

            try {
                srvPort = Integer.parseInt(srvPortStr);
            }
            catch (NumberFormatException ignore) {
            	context.close();
                throw new IgniteCheckedException("Failed to start Vertx server because IGNITE_JETTY_PORT system property cannot be cast to integer: " + srvPortStr);
            }            

            srvConn.setHost(System.getProperty(IGNITE_JETTY_HOST, "localhost"));
            srvConn.setPort(srvPort);
            srvConn.setIdleTimeout(60000);
            srvConn.setReuseAddress(true);

            httpSrv = new WebApiCreater(context,srvConn);
        }
        else {           

            try {
                this.props.load(new FileInputStream(cfgUrl.getFile()));
                
                String basePackages = this.props.getProperty("spring.web.scan.basePackages");
                if(basePackages!=null) {
                	context.scan(basePackages);
                }
            }
            catch (FileNotFoundException e) {
            	context.close();
                throw new IgniteSpiException("Failed to find configuration file: " + cfgUrl, e);
            }            
            catch (IOException e) {
            	context.close();
                throw new IgniteSpiException("Failed to load configuration file: " + cfgUrl, e);
            }
            catch (Exception e) {
            	context.close();
                throw new IgniteSpiException("Failed to start HTTP server with configuration file: " + cfgUrl, e);
            }
            
            try {
            	httpSrv = new WebApiCreater(context,this.props);
            }
            catch (Exception e) {
                throw new IgniteCheckedException("Failed to start Vertx HTTP server.", e);
            }
        }        
        
        assert httpSrv != null;
        
        int initPort =  httpSrv.options.getPort();
        int portRange = config().getPortRange();
        int lastPort = portRange == 0 ? initPort : initPort + portRange - 1;
        int port = initPort;
        for (port = initPort; port <= lastPort; port++) {
        	if(isPortInUse(port)) {
        		continue;
        	}
        	httpSrv.options.setPort(port);                          
            break;
        }
        
        serverSocketPlaceholder = new ServerSocket(port);
        
        override(httpSrv.options); 
        
    }

    /**     
     * Stops Vertx.
     */
    private void stopJetty() {
        // Vertx does not really stop the server if port is busy.
        try {
            if (httpSrv != null && httpSrv.isStarted()) {
                // If server was successfully started, deregister ports.
                
                ctx.ports().deregisterPorts(getClass());

                // Record current interrupted status of calling thread.
                boolean interrupted = Thread.interrupted();

                try {                	
                	httpSrv = null;
                }
                finally {
                    // Reset interrupted flag on calling thread.
                    if (interrupted)
                        Thread.currentThread().interrupt();
                }
            }
        }        
        catch (Exception e) {
            U.error(log, "Failed to stop Vertx HTTP server.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public void stop() {
    	handlerCount--;
    	

    	if(httpSrv!=null && httpSrv.isStarted()) {
    		
    		if(handlerCount <= 0) {
    	        stopJetty();	
    	        httpSrv = null;
        	}
        	 
        
        	if (log.isInfoEnabled())
                log.info(stopInfo());
    	}  
    	
    }

    /** {@inheritDoc} */
    @Override protected String getAddressPropertyName() {
        return IgniteNodeAttributes.ATTR_REST_JETTY_ADDRS;
    }

    /** {@inheritDoc} */
    @Override protected String getHostNamePropertyName() {
        return IgniteNodeAttributes.ATTR_REST_JETTY_HOST_NAMES;
    }

    /** {@inheritDoc} */
    @Override protected String getPortPropertyName() {
        return IgniteNodeAttributes.ATTR_REST_JETTY_PORT;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJettyRestProtocol.class, this);
    }
}
