package io.vertx.webmvc.creater.strategy;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.common.ResultDTO;
import io.vertx.webmvc.common.WebConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestParam;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.List;

/**
 * 处理RequestParam策略
 * @author zbw
 */
@Slf4j
@Service
public class RequestParamStrategy implements RouterStrategy {

    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i) {
        RequestParam requestParam = method.getParameters()[i].getAnnotation(RequestParam.class);
        String param;
        if ("\n\t\t\n\t\t\n\ue000\ue001\ue002\n\t\t\t\t\n".equals(requestParam.defaultValue())) {
            param = ctx.request().getParam(requestParam.value());
            if (param == null && requestParam.required()) {
                out(ctx, ResultDTO.failed("this parameter could not be null,maybe you should add a annotation:request=false"));
                return;
            }
        } else {
            param = "".equals(ctx.request().getParam(requestParam.value())) ? requestParam.defaultValue() : ctx.request().getParam(requestParam.value());
        }
        Object paramResult = typeConverter(param, method.getParameters()[i]);
        log.info("[vertx web] parameterType name is {},get RequestParam parameter:{},final result is:{}", method.getParameters()[i].getType().getTypeName(), param, paramResult);
        parameters[i]=(paramResult);
    }

    protected static void out(RoutingContext ctx, Object msg) {
        ctx.response()
                .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
                .end(JsonObject.mapFrom(msg).encode());
    }


    @Override
    public Class<? extends Annotation> getType() {
        return RequestParam.class;
    }


}
