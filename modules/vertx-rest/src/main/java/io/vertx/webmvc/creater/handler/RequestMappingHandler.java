package io.vertx.webmvc.creater.handler;


import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.lang.reflect.Method;

/**
 * 注解getMapping handle
 * @author zbw
 */
@Component
@Slf4j
public class RequestMappingHandler extends SingleRouterHandler {
	RequestMapping mapping;

    @Override
    public void doFilter(Method method, String prefix, Object classBean, Router router) {
        if (method.getAnnotation(RequestMapping.class) != null) {
            dealRouterTemplate(method, prefix, classBean, router);
        }
    }

    @Override
    public String getUrl(Method method) {
    	mapping = method.getAnnotation(RequestMapping.class);
        return "".equals(mapping.name()) ? mapping.value()[0] : mapping.name();
    }

    @Override
    public Route getRoute(Router router,String prefix,String url) {
        log.info("[vertx web] method api(get):" + prefix + url);
        Route r = router.route(prefix + url);
        for(RequestMethod m : mapping.method()) {
        	r = r.method(HttpMethod.valueOf(m.name()));
        }
        for(String contentType: mapping.consumes()) {
        	r = r.consumes(contentType);
        }
        for(String contentType: mapping.produces()) {
        	r = r.produces(contentType);
        }        
        return r;
    }

}
