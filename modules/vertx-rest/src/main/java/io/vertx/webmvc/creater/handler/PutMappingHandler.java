package io.vertx.webmvc.creater.handler;

import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PutMapping;

import java.lang.reflect.Method;

@Slf4j
@Component
public class PutMappingHandler extends SingleRouterHandler {

    @Override
    public void doFilter(Method method, String prefix, Object classBean, Router router) {
        if (method.getAnnotation(PutMapping.class) != null) {
            dealRouterTemplate(method, prefix, classBean, router);
        }
    }

    @Override
    public String getUrl(Method method) {
        return "".equals(method.getAnnotation(PutMapping.class).name()) ? method.getAnnotation(PutMapping.class).value()[0] : method.getAnnotation(PutMapping.class).name();
    }

    @Override
    public Route getRoute(Router router, String prefix, String url) {
        log.info("[vertx web] method api(put):" + prefix + url);
        return router.put(prefix + url);
    }
}
