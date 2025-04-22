package io.vertx.webmvc.creater.handler;

import io.vertx.ext.web.Router;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * 责任链对象连接handler
 * @author zbw
 */
@Component("InitSingleRouterHandler")
public class InitSingleRouterHandler {
    
    static List<SingleRouterHandler> singleRouterHandlerList = new ArrayList<>();

    private SingleRouterHandler singleRouterHandler = null;    
    

    /**
     * spring注入后自动执行，责任链的对象连接起来
     */    
    public void initSingleRouterFilter() {
        for (int i = 0; i < singleRouterHandlerList.size(); i++) {
            if (i == 0) {
                singleRouterHandler = singleRouterHandlerList.get(0);
            } else {
                SingleRouterHandler currentHander = singleRouterHandlerList.get(i - 1);
                SingleRouterHandler nextHander = singleRouterHandlerList.get(i);
                currentHander.setNextSingleRouterHandler(nextHander);
            }
        }
    }

    //直接调用这个方法使用
    public void exec(Method method, String prefix, Object classBean, Router router) {
    	if(singleRouterHandler==null) {
    		initSingleRouterFilter();
    	}
        singleRouterHandler.filter(method, prefix, classBean, router);
    }

}
