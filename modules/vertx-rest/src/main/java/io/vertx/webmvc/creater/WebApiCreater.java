package io.vertx.webmvc.creater;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.ErrorHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.webmvc.Vertxlet;
import io.vertx.webmvc.VertxInstanceAware;
import io.vertx.webmvc.annotation.VertxletMapping;
import io.vertx.webmvc.creater.handler.InitSingleRouterHandler;
import lombok.extern.slf4j.Slf4j;

import org.apache.ignite.internal.rest.igfs.util.FileUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * vert.x web API creater
 *
 * @author zbw
 */
@Slf4j
public class WebApiCreater extends AbstractVerticle {

    private Router router;    

	private ApplicationContext springContext;
    
    private Properties props = new Properties();

    private InitSingleRouterHandler initSingleRouterHandler;
    
    private final Map<String, Vertxlet> vertxletMap = new HashMap<>();
    
    private int state = 0; // 1: init, 2: ready, 3: started, 0: closed    

    public HttpServerOptions options = new HttpServerOptions();

    private HttpServer server;
    
    private Consumer<Router> readyCallback;
    
    protected String igniteInstanceName;

	public void setIgniteInstanceName(String igniteInstanceName) {
        this.igniteInstanceName = igniteInstanceName;
	}

	public WebApiCreater(ApplicationContext applicationContext,HttpServerOptions options) {
        springContext = applicationContext;
        this.options = options;        
    }
    
    public WebApiCreater(ApplicationContext applicationContext,Properties props) {
        springContext = applicationContext;
        this.props.putAll(props);
        configureFromProperties();
    }
    
    public WebApiCreater(ApplicationContext applicationContext) {
        springContext = applicationContext;
        configureFromProperties();
    }

    private void configureFromProperties() {
        String port = getProperty("server.port", "8080");
        String host = getProperty("server.host", "0.0.0.0");
        String maxBodySize = getProperty("server.maxBodySize", "10485760");
        String idleTimeout = getProperty("server.idleTimeout", "60");

        this.options.setPort(Integer.parseInt(port));
        this.options.setHost(host);
        this.options.setIdleTimeout(Integer.parseInt(idleTimeout));

        // 启用压缩
        boolean compressionSupported = Boolean.parseBoolean(getProperty("server.compressionSupported", "true"));
        this.options.setCompressionSupported(compressionSupported);
    }
    
    public Router getRouter() {
		return router;
	}
    
    public HttpServer getHttpServer() {
    	return server;
    }
    
    public boolean isClosed() {
    	return state==0;
    }
   
    public boolean isInit() {
    	return state==1;
    }
    
    public boolean isReady() {
    	return state==2;
    }
    
    public boolean isStarted() {
    	return state==3;
    }
    
    
    public String getProperty(String name,String def) {
    	String value = props.getProperty(name); 
    	if(value==null) {
    		value = springContext.getEnvironment().getProperty(name);		
    	}
    	return value!=null? value: def;
    }
    
    public String getProperty(String name) {
    	return getProperty(name,null);
    }
    
    public Consumer<Router> getReadyCallback() {
		return readyCallback;
	}

	public void setReadyCallback(Consumer<Router> readyCallback) {
		this.readyCallback = readyCallback;
	}

    @Override
    public void start(Promise<Void> startPromise) {
    	if(state>0) {
    		log.warn("[WebAPiCreater] already started");
    		return;
    	}
    	state = 1;
        log.info("[WebAPiCreater] start...");
        
        String staticDirs = getProperty("spring.web.resources.static-locations");
        
        server = vertx.createHttpServer(options);
        router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().last().failureHandler(ErrorHandler.create(vertx));

        cros(router);
        //将所有的controller转换成router
        try {

            initSingleRouterHandler =  this.springContext.getBean(InitSingleRouterHandler.class);
            // 获取所有beanNames
            String[] beanNames = this.springContext.getBeanNamesForType((Class)null);
            for (String beanName : beanNames) {
            	try {
	                RestController restController = this.springContext.findAnnotationOnBean(beanName, RestController.class);
	                //判断该类是否含有RestController注解
	                if (restController != null) {
	                    RequestMapping requestMapping = this.springContext.findAnnotationOnBean(beanName,RequestMapping.class);
	                    String prefixName;
	                    if (requestMapping == null) {
	                        prefixName = "";
	                    } else {
	                        prefixName = "".equals(requestMapping.name()) ? requestMapping.value()[0] : requestMapping.name();
	                    }
	                    log.info("[vertx web] prefix:" + prefixName);
	                    Object newInstance = this.springContext.getBean(beanName);
	                    if(newInstance instanceof VertxInstanceAware) {
	                    	VertxInstanceAware vertRestController = (VertxInstanceAware)newInstance;
	                    	vertRestController.setVertx(vertx);
	                    	vertRestController.setIgniteInstanceName(igniteInstanceName);
	                    }
	                    
	                    Method[] methods = ReflectionUtils.getAllDeclaredMethods(newInstance.getClass());
	                    for (Method method : methods) {
	                        initSingleRouterHandler.exec(method,prefixName,newInstance,router);
	                    }
	                }
	                
	                VertxletMapping vertxletController = this.springContext.findAnnotationOnBean(beanName, VertxletMapping.class);
	                //判断该类是否含有VertxletMapping注解
	                if (vertxletController != null) {                    
	                    
	                	Vertxlet vertxlet = this.springContext.getBean(beanName,Vertxlet.class);
	                	vertxlet.setVertx(vertx);
	                	vertxlet.setIgniteInstanceName(igniteInstanceName);
	                    for (String url : vertxlet.getClass().getAnnotation(VertxletMapping.class).url()) {
	                        log.info("Mapping url {} with class {}", url, vertxlet.getClass().getName());
	                        vertxletMap.put(url, vertxlet);
	                        router.route(url).blockingHandler(vertxlet);
	                    }
	                }
            	}
            	catch (Exception e) {
            		log.error("[vertx web] get web beans:", e);
                }
            	
            }

            if(staticDirs!=null && !staticDirs.isEmpty()) {
                java.util.Arrays.stream(staticDirs.split(",")).forEach(staticDir -> {
                    log.info("[vertx web] staticDir:" + staticDir);
                    String contextPath = FileUtil.getFilename(staticDir);
                    router.route("/"+contextPath).blockingHandler(StaticHandler.create(staticDir.trim()));
                });
            }

            log.info("[Vertx web] Create web for Spring-web with vertx web succeed!");
        } catch (Exception e) {
            throw new RuntimeException(e.toString());
        }
        
        server = server.requestHandler(router);
        
        initializeVertxlet(startPromise);
        
        state = 2; // binding
        
        if(readyCallback!=null) {
        	readyCallback.accept(router);
        }        
       
    	server.listen().andThen(res->{
        	if (res.succeeded()) {
        		state = 3;
                log.info("Http server started at [{}:{}]",  options.getHost(), options.getPort());
                
            } else {
                startPromise.fail(res.cause());
                state = 0;
            }
        });
    }
    
    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
    	state = 0;
        destroyVertxlet(stopPromise);
        server.close();
    }
   
    
    private void cros(Router router) {
        Set<String> allowedHeaders = new HashSet<>(
                List.of("Mcp-Session-Id","Last-Event-ID","x-requested-with","X-PINGARUNER"));

        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("Origin");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("Accept");
        allowedHeaders.add("Authorization");

        allowedHeaders.add("*");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.OPTIONS);
        allowedMethods.add(HttpMethod.DELETE);
        allowedMethods.add(HttpMethod.PATCH);
        allowedMethods.add(HttpMethod.PUT);

        String allowedOrigins = getProperty("cors.allowedOrigins", "*");

        router.route().handler(CorsHandler.create()
                .addOrigin(allowedOrigins)
                .allowedHeaders(allowedHeaders)
                .exposedHeader("ETag")
                .allowedMethods(allowedMethods));
    }
    
    private void initializeVertxlet(Promise<Void> startPromise) {
        AtomicInteger count = new AtomicInteger(vertxletMap.size());
        if (count.get() == 0) {
            startPromise.complete();
            return;
        }

        for (String key : vertxletMap.keySet()) {
            Promise<Void> promise = Promise.promise();
            vertx.runOnContext(v -> vertxletMap.get(key).init(promise));
            promise.future().onComplete(ar -> {
                if (ar.succeeded()) {
                    if (count.decrementAndGet() == 0) {
                        log.info("All vertxlet initialized");
                        startPromise.complete();
                    }
                } else {
                    startPromise.fail(ar.cause());
                }
            });
        }
    }

    private void destroyVertxlet(Promise<Void> stopPromise) {
        AtomicInteger count = new AtomicInteger(vertxletMap.size());
        if (count.get() <= 0) {
            stopPromise.complete();
            return;
        }
        vertxletMap.forEach((name, vertxlet) -> {
            Promise<Void> promise = Promise.promise();
            vertx.runOnContext(v -> vertxlet.destroy(promise));
            promise.future().onComplete(ar -> {
                if (ar.succeeded()) {
                    if (count.decrementAndGet() == 0) {
                        log.info("All vertxlet destroy succeeded");
                        stopPromise.complete();
                    }
                } else {
                    stopPromise.fail(ar.cause());
                }
            });
        });
    }
}
