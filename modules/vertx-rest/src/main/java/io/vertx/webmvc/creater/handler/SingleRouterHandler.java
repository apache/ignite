package io.vertx.webmvc.creater.handler;

import cn.hutool.core.convert.Convert;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.ClusterSerializable;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.annotation.Blocking;
import io.vertx.webmvc.common.ResultDTO;
import io.vertx.webmvc.common.WebConstant;
import io.vertx.webmvc.creater.factory.RequestFactory;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;


import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 路由handler通用模板方式
 * @author zbw
 */
@Slf4j
@Component
public abstract class SingleRouterHandler {

	@Autowired
    private RequestFactory requestFactory;
	
	@Autowired
	private ConversionService conversionService;

    private SingleRouterHandler nextSingleRouterHandler;
    
    protected SingleRouterHandler() {
    	InitSingleRouterHandler.singleRouterHandlerList.add(this);
    }

    public static void out(RoutingContext ctx, Object msg) {
    	if(msg==null) {
    		ctx.response().end();
    		return;
    	}
    	
        if(msg instanceof byte[]) {
            Buffer data = Buffer.buffer((byte[]) msg);
            ctx.response()
                    .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.OCTET_MEDIA_TYPE)
                    .end(data);
        }
        else if(msg instanceof char[]) {
            String data = String.valueOf((char[]) msg);
            ctx.response()
                    .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.TEXT_MEDIA_TYPE)
                    .end(data);
        }
        else if(msg instanceof CharSequence){
            String output = msg.toString();
            ctx.response()                    
                    .end(output);
        }
        else if(msg.getClass().isArray()) {
        	if(!msg.getClass().getComponentType().isPrimitive()) {
            	Object[] list = (Object[]) msg;
        		String output = JsonArray.of(list).encodePrettily();
        		ctx.response()
        			.putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
        			.end(output);
        	}
        	else {
        		Class<?> comType= msg.getClass().getComponentType();
        		String output = "";
        		if(comType==int.class) {
        			output = Arrays.toString((int[])msg);
        		}
        		else if(comType==long.class) {
        			output = Arrays.toString((long[])msg);
        		}
        		else if(comType==short.class) {
        			output = Arrays.toString((short[])msg);
        		}
        		else if(comType==float.class) {
        			output = Arrays.toString((float[])msg);
        		}
        		else if(comType==double.class) {
        			output = Arrays.toString((double[])msg);
        		}
        		else if(comType==boolean.class) {
        			output = Arrays.toString((boolean[])msg);
        		}
                ctx.response()
                        .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
                        .end(output);
        	}
    	}
        else if(msg instanceof List) {
        	List list = (List) msg;
    		String output = new JsonArray(list).encodePrettily();
    		ctx.response()
	    		.putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
	    		.end(output);
    	}
        else if(msg instanceof ResponseEntity) {
        	ResponseEntity res = (ResponseEntity)msg;
        	ctx.response().setStatusCode(res.getStatusCodeValue());
        	if(res.getHeaders()!=null) {        		
        		res.getHeaders().forEach((k,v)->{
        			ctx.response().putHeader(k, v);
            	});
        	}
        	
        	out(ctx,res.getBody());
        }
        else if(msg instanceof ClusterSerializable){
        	ClusterSerializable json = (ClusterSerializable)msg;
        	Buffer buffer = Buffer.buffer(1024);
        	json.writeToBuffer(buffer);
            ctx.response()
                    .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
                    .end(buffer);
        }
        else{
            String output = JsonObject.mapFrom(msg).encodePrettily();
            ctx.response()
                    .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
                    .end(output);
        }

    }

    public SingleRouterHandler getNextSingleRouterHandler() {
        return nextSingleRouterHandler;
    }

    public void setNextSingleRouterHandler(SingleRouterHandler nextSingleRouterHandler) {
        this.nextSingleRouterHandler = nextSingleRouterHandler;
    }

    public void filter(Method method, String prefix, Object classBean, Router router) {
        doFilter(method, prefix, classBean, router);
        if (getNextSingleRouterHandler() != null) {
            getNextSingleRouterHandler().filter(method, prefix, classBean, router);
        }
    }

    public abstract void doFilter(Method method, String prefix, Object classBean, Router router);

    public void dealRouterTemplate(Method method, String prefix, Object classBean, Router router) {    	
        String url = getUrl(method);
        boolean isAync = Future.class.isAssignableFrom(method.getReturnType()) || java.util.concurrent.Future.class.isAssignableFrom(method.getReturnType());
        if((method.getAnnotation(Blocking.class) != null || classBean.getClass().getAnnotation(Blocking.class) != null) && !isAync) {
        	
        	getRoute(router, prefix, url).blockingHandler(ctx -> webHandle(ctx, method, prefix, url, classBean))
	            .failureHandler(frc -> {
	                log.warn("[vertx web]The API name:{} invoke failed,the reason is {}", prefix + url, frc.failure().toString());
	                out(frc, frc.toString());
	            });
        }
        else {
        		
	        getRoute(router, prefix, url).handler(ctx -> webHandle(ctx, method, prefix, url, classBean))
                .failureHandler(frc -> {
                    log.warn("[vertx web]The API name:{} invoke failed,the reason is {}", prefix + url, frc.failure().toString());
                    out(frc, frc.toString());
                });
        }

    }

    public abstract String getUrl(Method method);

    public abstract Route getRoute(Router router, String prefix, String url);

    protected void webHandle(RoutingContext ctx, Method method, String prefix, String url, Object classBean) {
        //该方法所有的参数
        Object[] parameters = new Object[method.getParameterCount()];
        //遍历该方法的所有参数注解（注：可能有不带注解的参数）
        for (int i = 0; i < method.getParameterCount(); i++) {
            List<Class<? extends Annotation>> parameterAnnotations = getParameterAnnotations(method, i);
            requestFactory.dealRouter(parameterAnnotations, ctx, method, parameters, i);
        }
        //遍历该方法的所有注解（注：可能有不带注解的方法）
        List<Class<? extends Annotation>> parameterAnnotations = getMethodAnnotations(method);
        requestFactory.dealMethodRouter(parameterAnnotations, ctx, method, parameters);

        log.info("[vertx web] objects:{}", Arrays.toString(parameters));
        log.info("[vertx web] api has benn invoke.The api name is:" + prefix + url);
        boolean isAync = Future.class.isAssignableFrom(method.getReturnType()) || java.util.concurrent.Future.class.isAssignableFrom(method.getReturnType());
        if (isAync || "void".equalsIgnoreCase(method.getReturnType().getTypeName())) {
            log.info("[vertx web] this method returnType is void");
            try {
                //method.invoke(classBean,parameters);
                ReflectionUtils.invokeMethod(method, classBean, parameters);
            } catch (Exception e) {
            	if(e instanceof IllegalArgumentException) {
            		ctx.response().setStatusCode(404);
            	}
            	else {
            		ctx.response().setStatusCode(500);
            	}
            	ctx.response().setStatusMessage(e.getMessage());
            	ctx.response().end();
                
            }
            
        } else {
            log.info("[vertx web] this method returnType isn't void");
            try {
                out(ctx, ReflectionUtils.invokeMethod(method, classBean, parameters));

            } catch (Exception e) {
            	if(e instanceof IllegalArgumentException) {
            		ctx.response().setStatusCode(404);
            	}
            	else {
            		ctx.response().setStatusCode(500);
            	}
            	ctx.response().setStatusMessage(e.getMessage());
            	ctx.response().end();
            }
        }
    }

    private List<Class<? extends Annotation>> getParameterAnnotations(Method method, int i) {
        return Arrays
                .stream(method.getParameterAnnotations()[i])
                .map(Annotation::annotationType)
                .collect(Collectors.toList());
    }

    private List<Class<? extends Annotation>> getMethodAnnotations(Method method) {
        return Arrays
                .stream(method.getAnnotations())
                .map(Annotation::annotationType)
                .collect(Collectors.toList());
    }

    protected Object typeConverter(Object obj, Parameter parameter) {
    	
        try {
        	if(obj!=null && conversionService.canConvert(obj.getClass(), parameter.getType())) {
        		return conversionService.convert(obj, parameter.getType());
        	}
        	
            return Convert.convertQuietly(parameter.getType(), obj, null);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return obj;
    }

}
