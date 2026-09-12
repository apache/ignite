package io.vertx.webmvc.creater.handler;

import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.DeleteMapping;

import java.lang.reflect.Method;

/**
 * 注解deleteMapping handle
 * @author zbw
 */
@Component
@Slf4j
public class DeleteMappingHandler extends SingleRouterHandler {

    @Override
    public void doFilter(Method method, String prefix, Object classBean, Router router) {
        if (method.getAnnotation(DeleteMapping.class) != null) {
            dealRouterTemplate(method, prefix, classBean, router);
        }
    }

    @Override
    public String getUrl(Method method) {
        return "".equals(method.getAnnotation(DeleteMapping.class).name()) ? method.getAnnotation(DeleteMapping.class).value()[0] : method.getAnnotation(DeleteMapping.class).name();
    }

    @Override
    public Route getRoute(Router router, String prefix, String url) {
        log.info("[vertx web] method api(delete):" + prefix + url);
        return router.delete(prefix + url);
    }
}
