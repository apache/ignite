package io.vertx.webmvc.creater.strategy;

import io.vertx.core.buffer.Buffer;
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
public class RequestBodyStrategy extends RouterStrategy {
    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i) {
        Class<?> parameterClass = method.getParameterTypes()[i];
        if(parameterClass==String.class) {
        	parameters[i]= ctx.body().asString();
        }
        else if(parameterClass==byte[].class) {
        	parameters[i]= ctx.body().buffer().getBytes();
        }
        else if(parameterClass==Buffer.class) {
        	parameters[i]= ctx.body().buffer();
        }
        else if(parameterClass==List.class) {
        	parameters[i]= (ctx.body().asJsonArray().getList());
        }
        else if(parameterClass.isArray()) {
        	parameters[i]= (ctx.body().asJsonArray().getList().toArray());
        }
        else {
        	parameters[i]= (ctx.body().asJsonObject().mapTo(parameterClass));
        }
        log.info("[vertx web] get RequestBody parameter:{}", parameterClass);
    }

    @Override
    public Class<? extends Annotation> getType() {
        return RequestBody.class;
    }


}
