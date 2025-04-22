package io.vertx.webmvc.creater.handler;

import cn.hutool.core.convert.Convert;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.annotation.Blocking;
import io.vertx.webmvc.common.ResultDTO;
import io.vertx.webmvc.common.WebConstant;
import io.vertx.webmvc.creater.factory.RequestFactory;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.bind.annotation.GetMapping;

import javax.annotation.Resource;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    private SingleRouterHandler nextSingleRouterHandler;
    
    protected SingleRouterHandler() {
    	InitSingleRouterHandler.singleRouterHandlerList.add(this);
    }

    public static void out(RoutingContext ctx, Object msg) {
        if(msg instanceof byte[]) {
            Buffer data = Buffer.buffer((byte[]) msg);
            ctx.response()
                    .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.OCTET_MEDIA_TYPE)
                    .end(data);
        }
        else if(msg instanceof CharSequence){
            String output = msg.toString();
            ctx.response()
                    .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.TEXT_MEDIA_TYPE)
                    .end(output);
        }
        else{
            String output = JsonObject.mapFrom(msg).encode();
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
        if(method.getAnnotation(Blocking.class) != null) {
        	
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
        if ("void".equals(method.getReturnType().getTypeName())) {
            log.info("[vertx web] this method returnType is void");
            try {
                //method.invoke(classBean,parameters);
                ReflectionUtils.invokeMethod(method, classBean, parameters);
            } catch (Exception e) {
                throw new RuntimeException(e.toString());
            }
            out(ctx, ResultDTO.success("ok"));
        } else {
            log.info("[vertx web] this method returnType isn't void");
            try {
                out(ctx, ReflectionUtils.invokeMethod(method, classBean, parameters));

            } catch (Exception e) {
                e.printStackTrace();
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

    protected Object typeConverter(String obj, Parameter parameter) {
        try {
            return Convert.convertQuietly(parameter.getType(), obj, null);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return obj;
    }

}
