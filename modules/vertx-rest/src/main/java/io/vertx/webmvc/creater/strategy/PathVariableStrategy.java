package io.vertx.webmvc.creater.strategy;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.common.ResultDTO;
import io.vertx.webmvc.common.WebConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;

/**
 * 处理PathVariable策略
 * @author zbw
 */
@Slf4j
@Service
public class PathVariableStrategy implements RouterStrategy {

    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i) {
        // RequestMapping requestMapping = method.getAnnotation(RequestMapping.class);
        PathVariable requestParam = method.getParameters()[i].getAnnotation(PathVariable.class);
        String paramName = requestParam.value();
        String param = ctx.pathParam(paramName);
        if(param==null){
            out(ctx, ResultDTO.failed("this path variable could not be null!"));
            return;
        }
        Object paramResult = typeConverter(param, method.getParameters()[i]);
        log.info("[vertx web] parameterType name is {},get RequestParam parameter:{},final result is:{}", method.getParameters()[i].getType().getTypeName(), param, paramResult);
        parameters[i] = paramResult;
    }

    protected static void out(RoutingContext ctx, Object msg) {
        ctx.response()
                .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
                .end(JsonObject.mapFrom(msg).encode());
    }


    @Override
    public Class<? extends Annotation> getType() {
        return PathVariable.class;
    }


}
