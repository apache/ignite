package io.vertx.webmvc.creater.strategy;

import io.vertx.core.http.Cookie;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.annotation.AuthedUser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.List;

@Slf4j
@Service
public class AuthedUserStrategy implements RouterStrategy {

    @Override
    public void dealRouter(RoutingContext ctx, Method method, Object[] parameters, int i) {
        log.info("path: " + ctx.request().path());
        Cookie cookie = ctx.request().getCookie("session_id");
        if(cookie!=null){

        }
    }

    @Override
    public Class<? extends Annotation> getType() {
        return AuthedUser.class;
    }
}
