package io.vertx.webmvc.creater.strategy.patch;

import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.annotation.AuthedUser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

@Slf4j
@Service
public class PatchAuthedUser implements PatchRouter {
    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters) {
        log.info("path: " + ctx.request().path());
        User user = ctx.user();
        if(user!=null){        	
            log.info("[vertx web] method name:{}, user:{}", method.getName(), user);            
        }
    }

    @Override
    public Class<? extends Annotation> getType() {
        return AuthedUser.class;
    }
}
