package io.vertx.webmvc.creater.strategy;

import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestBody;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;

/**
 * 处理RequestBody策略
 * @author zbw
 */
@Slf4j
@Service
public class RequestBodyStrategy implements RouterStrategy {
    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i) {
        Class<?> parameterClass = method.getParameterTypes()[i];
        parameters[i]= (ctx.getBodyAsJson().mapTo(parameterClass));
        log.info("[vertx web] get RequestBody parameter:{}", ctx.getBodyAsJson().mapTo(parameterClass));
    }

    @Override
    public Class<? extends Annotation> getType() {
        return RequestBody.class;
    }


}
