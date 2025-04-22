package io.vertx.webmvc.creater.strategy;

import cn.hutool.core.convert.Convert;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.common.WebConstant;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;


import java.lang.reflect.Method;
import java.util.List;

/**
 * 通用策略定义
 * @author zbw
 */
public interface RouterStrategy {

    void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i);

    Class<? extends Annotation> getType();


    default Object typeConverter(String obj, Parameter parameter) {
        try {
            return Convert.convertQuietly(parameter.getType(), obj, null);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        return obj;
    }

}
