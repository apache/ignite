package io.vertx.webmvc.creater.strategy.patch;

import io.vertx.ext.web.RoutingContext;

import java.lang.annotation.*;
import java.lang.reflect.Method;

/**
 * @author zbw
 */

public interface PatchRouter {
    void dealRouter(RoutingContext ctx, Method method, Object[] parameters);

    Class<? extends Annotation> getType();
}
