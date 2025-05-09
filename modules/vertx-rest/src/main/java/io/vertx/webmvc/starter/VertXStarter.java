package io.vertx.webmvc.starter;


import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;
import io.vertx.webmvc.creater.WebApiCreater;
import io.vertx.webmvc.utils.NetUtils;
import lombok.extern.slf4j.Slf4j;

import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.plugin.IgniteVertxPlugin;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Properties;

import javax.annotation.PostConstruct;

/**
 * vert.x starter
 *
 * @author zbw
 */
@Slf4j
public class VertXStarter implements ApplicationContextAware {
    public Vertx vertx;
    private ApplicationContext springContext;   
    private WebApiCreater webApiCreater;
    
    public WebApiCreater getWebApiCreater() {
		return webApiCreater;
	}

	public void setWebApiCreater(WebApiCreater webApiCreater) {
		this.webApiCreater = webApiCreater;
	}

	@Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.springContext = applicationContext;
    }

    @PostConstruct
    public void init() {
        long startwatch = System.currentTimeMillis();
        log.info("[Vertx web] Vertx web's vert.x system start to bootstrap...");
        int port = springContext.getEnvironment().getProperty("server.port", Integer.class);
        String localIP = NetUtils.getLocalHost();
        String address = localIP + ":" + port;
        log.info("[Vertx web] vert.x server address: {}", address);
        if(webApiCreater!=null) {
	        vertx = Vertx.vertx();
	        vertx.deployVerticle(webApiCreater);
	        long stopwatch = System.currentTimeMillis();
	        log.info("[Vertx web] Vertx web's vert.x system started successfully, using time {}s.", (stopwatch-startwatch)/1000);
        }
    }
    
	public Future<String> start(Ignite ignite) {
		IgniteVertxPlugin plugin = ignite.plugin("Vertx");		
        if(webApiCreater!=null) {
        	if(ignite.cluster().state()==ClusterState.INACTIVE) {
        		vertx = Vertx.vertx();
        	}
        	else {
        		vertx = plugin.vertx();
        	}
        	return vertx.deployVerticle(webApiCreater);
        }
        return null;
	}
	
	public void stop(Ignite ignite) {
		IgniteVertxPlugin plugin = ignite.plugin("Vertx");		
        if(webApiCreater!=null) {        	
        	
        }
	}
}
