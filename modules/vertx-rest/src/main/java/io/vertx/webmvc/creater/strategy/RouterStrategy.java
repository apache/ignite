package io.vertx.webmvc.creater.strategy;

import cn.hutool.core.convert.Convert;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.common.WebConstant;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;


import java.lang.reflect.Method;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.convert.ConversionService;
import org.springframework.format.support.FormattingConversionService;

/**
 * 通用策略定义
 * @author zbw
 */
public abstract class RouterStrategy {
	
	@Autowired
	private ConversionService conversionService;

	public abstract void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i);

	public abstract Class<? extends Annotation> getType();
    
    
    public Object typeConverter(Object obj, Parameter parameter) {
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
    
    protected static void out(RoutingContext ctx, Object msg) {
        ctx.response()
                .putHeader(WebConstant.HTTP_HEADER_CONTENT_TYPE, WebConstant.JSON_MEDIA_TYPE)
                .end(JsonObject.mapFrom(msg).encode());
    }


}
