package io.vertx.webmvc.creater.factory;


import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.creater.strategy.RouterStrategy;
import io.vertx.webmvc.creater.strategy.UnlessStrategy;
import io.vertx.webmvc.creater.strategy.patch.PatchRouter;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 请求参数解析方式工厂
 * @author zbw
 */
@Component
public class RequestFactory implements ApplicationContextAware {

    @Autowired
    UnlessStrategy unless;

    private Map<Class<? extends Annotation>, RouterStrategy> map = new ConcurrentHashMap<>();

    private Map<Class<? extends Annotation>, PatchRouter> patchedMap = new ConcurrentHashMap<>();
    
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, RouterStrategy> tempMap = applicationContext.getBeansOfType(RouterStrategy.class);
        tempMap.values().forEach(routerStrategy -> map.put(routerStrategy.getType(), routerStrategy));

        Map<String, PatchRouter> patchMap = applicationContext.getBeansOfType(PatchRouter.class);
        for (PatchRouter routerPatch : patchMap.values()) {
            patchedMap.put(routerPatch.getType(), routerPatch);
        }
    }

    public void dealRouter(List<Class<? extends Annotation>> parameterAnnotations, RoutingContext ctx, Method method, Object[] parameters, int i) {

        map.get(parameterAnnotations.stream()
                .filter(parameterAnnotation -> map.get(parameterAnnotation) != null)
                .findAny().orElse(unless.getType()))
                .dealRouter(ctx, method, parameters, i);
    }

    public long dealMethodRouter(List<Class<? extends Annotation>> parameterAnnotations, RoutingContext ctx, Method method, Object[] parameters){
        long processed = parameterAnnotations.stream()
                .filter(parameterAnnotation -> patchedMap.get(parameterAnnotation) != null)
                .map(parameterAnnotation -> patchedMap.get(parameterAnnotation))
                .map(patch->{
                    patch.dealRouter(ctx, method, parameters);
                    return 1;
                })
                .count();

        return processed;
    }
}
