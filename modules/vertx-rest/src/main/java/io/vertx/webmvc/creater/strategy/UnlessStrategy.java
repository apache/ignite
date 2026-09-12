package io.vertx.webmvc.creater.strategy;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.Session;
import io.vertx.webmvc.annotation.Unless;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;

/**
 * 独特策略
 * @author zbw
 */
@Slf4j
@Service
public class UnlessStrategy extends RouterStrategy {

    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i) {
    	Class<?> parameterClass = method.getParameterTypes()[i];
    	if (parameterClass==RoutingContext.class) {
    		parameters[i] = ctx;
    		return;
    	}
    	if (parameterClass==HttpServerRequest.class) {
    		parameters[i] = ctx.request();
    		return;
    	}
    	if (parameterClass==HttpServerResponse.class) {
    		parameters[i] = ctx.response();
    		return;
    	}
    	if (parameterClass==Session.class) {
    		parameters[i] = ctx.session();
    		return;
    	}
        log.warn("[vertx web] no annotation in this parameter,please add annotation :RequestParam");
        String paramName = method.getParameters()[i].getName();
        String param = ctx.request().getParam(paramName);
        Object paramResult = typeConverter(param, method.getParameters()[i]);
        log.info("[vertx web] parameter name is {},get RequestParam parameter:{},final result is:{}", paramName, param, paramResult);
        parameters[i]=(paramResult);
        return;
    }

    @Override
    public Class<? extends Annotation> getType() {
        return Unless.class;
    }


}
