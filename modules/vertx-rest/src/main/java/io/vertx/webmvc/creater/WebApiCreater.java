package io.vertx.webmvc.creater;

import io.vertx.core.AbstractVerticle;
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
import io.vertx.webmvc.annotation.VertxletMapping;
import io.vertx.webmvc.creater.handler.InitSingleRouterHandler;
import io.vertx.webmvc.utils.SpringUtils;
import lombok.extern.slf4j.Slf4j;

import org.apache.ignite.internal.processors.rest.igfs.util.FileUtil;
import org.springframework.context.ApplicationContext;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.*;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
    
    public HttpServerOptions options = new HttpServerOptions();
    
    private Properties props = new Properties();

    private InitSingleRouterHandler initSingleRouterHandler = (InitSingleRouterHandler) SpringUtils.getBean(InitSingleRouterHandler.class);
    
    private final Map<String, Vertxlet> vertxletMap = new HashMap<>();
    
    private int state = 0; // 1: init, 2: ready, 3: started, 0: closed    
    
    private HttpServer server;
    
    private Consumer<Router> readyCallback;


    

	public WebApiCreater(ApplicationContext applicationContext,HttpServerOptions options) {
        springContext = applicationContext;
        this.options = options;        
    }
    
    public WebApiCreater(ApplicationContext applicationContext,Properties props) {
        springContext = applicationContext;
        this.props.putAll(props);
        this.options = new HttpServerOptions();
        String port = getProperty("server.port");
        String host = getProperty("server.host");
        if(port!=null) {
        	this.options.setPort(Integer.parseInt(port));
        }
        if(host!=null) {
        	this.options.setHost(host);
        }
    }
    
    public WebApiCreater(ApplicationContext applicationContext) {
        springContext = applicationContext;
        this.options = new HttpServerOptions();
        Integer port = springContext.getEnvironment().getProperty("server.port", Integer.class);
        String host = springContext.getEnvironment().getProperty("server.host", String.class);
        if(port!=null) {
        	this.options.setPort(port);
        }
        if(host!=null) {
        	this.options.setHost(host);
        }
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
        
        HttpServer server = vertx.createHttpServer(options);

        router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        router.route().last().failureHandler(ErrorHandler.create(vertx));

        cros(router);
        //将所有的controller转换成router
        try {
            // 获取所有beanNames
            String[] beanNames = SpringUtils.getBeanNamesForType(Object.class);
            for (String beanName : beanNames) {
            	try {
	                RestController restController = SpringUtils.findAnnotationOnBean(beanName, RestController.class);
	                //判断该类是否含有RestController注解
	                if (restController != null) {
	                    RequestMapping requestMapping = SpringUtils.findAnnotationOnBean(beanName,RequestMapping.class);
	                    String prefixName;
	                    if (requestMapping == null) {
	                        prefixName = "";
	                    } else {
	                        prefixName = "".equals(requestMapping.name()) ? requestMapping.value()[0] : requestMapping.name();
	                    }
	                    log.info("[vertx web] prefix:" + prefixName);
	                    Object newInstance = SpringUtils.getBean(beanName);
	                    Method[] methods = ReflectionUtils.getAllDeclaredMethods(newInstance.getClass());
	                    for (Method method : methods) {
	                        initSingleRouterHandler.exec(method,prefixName,newInstance,router);
	                    }
	                }
	                
	                VertxletMapping vertxletController = SpringUtils.findAnnotationOnBean(beanName, VertxletMapping.class);
	                //判断该类是否含有VertxletMapping注解
	                if (vertxletController != null) {                    
	                    
	                	Vertxlet vertxlet = SpringUtils.getBean(beanName,Vertxlet.class);
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
       
    	server.listen(options.getPort(),options.getHost()).andThen(res->{
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
    }
   
    
    private void cros(Router router) {
        Set<String> allowedHeaders = new HashSet<>();
        allowedHeaders.add("x-requested-with");
        allowedHeaders.add("Access-Control-Allow-Origin");
        allowedHeaders.add("Origin");
        allowedHeaders.add("Content-Type");
        allowedHeaders.add("Accept");
        allowedHeaders.add("X-PINGARUNER");

        Set<HttpMethod> allowedMethods = new HashSet<>();
        allowedMethods.add(HttpMethod.GET);
        allowedMethods.add(HttpMethod.POST);
        allowedMethods.add(HttpMethod.OPTIONS);
        /*
         * these methods aren't necessary for this sample,
         * but you may need them for your projects
         */
        allowedMethods.add(HttpMethod.DELETE);
        allowedMethods.add(HttpMethod.PATCH);
        allowedMethods.add(HttpMethod.PUT);

        router.route().handler(CorsHandler.create().addOrigin("*").allowedHeaders(allowedHeaders).allowedMethods(allowedMethods));
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
