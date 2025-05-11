package io.vertx.webmvc.creater.handler;


import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PostMapping;

import java.io.Serializable;
import java.lang.reflect.Method;

@Component
@Slf4j
public class PostMappingHandler extends SingleRouterHandler {

    @Override
    public void doFilter(Method method, String prefix, Object classBean, Router router) {
        if (method.getAnnotation(PostMapping.class) != null) {
            dealRouterTemplate(method, prefix, classBean, router);
        }
    }

    @Override
    public String getUrl(Method method) {
        return "".equals(method.getAnnotation(PostMapping.class).name()) ? method.getAnnotation(PostMapping.class).value()[0] : method.getAnnotation(PostMapping.class).name();
    }

    @Override
    public Route getRoute(Router router, String prefix, String url) {
        log.info("[vertx web] method api(post):" + prefix + url);
        return router.post(prefix + url);
    }


}
