package io.vertx.webmvc.creater.strategy;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.common.ResultDTO;
import io.vertx.webmvc.common.WebConstant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.ValueConstants;

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
public class RequestHeaderStrategy extends RouterStrategy {

    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i) {
    	RequestHeader requestParam = method.getParameters()[i].getAnnotation(RequestHeader.class);
        String param;
        if (ValueConstants.DEFAULT_NONE.equals(requestParam.defaultValue())) {
            param = ctx.request().getHeader(requestParam.value());
            if (param == null && requestParam.required()) {
                out(ctx, ResultDTO.failed("this parameter could not be null,maybe you should add a annotation:request=false"));
                return;
            }
        } else {
            param = "".equals(ctx.request().getHeader(requestParam.value())) ? requestParam.defaultValue() : ctx.request().getHeader(requestParam.value());
        }
        Object paramResult = typeConverter(param, method.getParameters()[i]);
        log.info("[vertx web] parameterType name is {},get RequestParam parameter:{},final result is:{}", method.getParameters()[i].getType().getTypeName(), param, paramResult);
        parameters[i]=(paramResult);
    }
    

    @Override
    public Class<? extends Annotation> getType() {
        return RequestHeader.class;
    }


}
