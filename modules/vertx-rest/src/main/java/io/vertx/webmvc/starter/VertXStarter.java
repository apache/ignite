package io.vertx.webmvc.starter;


import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.webmvc.creater.WebApiCreater;
import io.vertx.webmvc.utils.NetUtils;
import lombok.extern.slf4j.Slf4j;

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
    private final String contexPath;
    
    public VertXStarter(String contexPath) {
    	this.contexPath = contexPath;
    }
    
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

	public String getContexPath() {
		return contexPath;
	}
}
