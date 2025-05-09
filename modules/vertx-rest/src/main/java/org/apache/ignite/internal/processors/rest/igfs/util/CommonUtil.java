package org.apache.ignite.internal.processors.rest.igfs.util;


import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;

import java.util.UUID;

public class CommonUtil {
    public static String getNewGuid() {
        String dateStr = DateUtil.getDateTagToSecond();
        String randomStr = UUID.randomUUID().toString();
        try{
            randomStr = EncryptUtil.encryptByMD5(randomStr).toLowerCase();
        }catch (Exception e){
            e.printStackTrace();
        }
        return dateStr + randomStr;
    }

    public static String getApiPath(HttpServerRequest request) {
        String contextPath = request.path();
        String scheme = request.scheme();
        String servaerName = request.authority().host();
        int port = request.authority().port();
        String rootPageURL = scheme + ":" + "//" + servaerName + ":" + port + "/" + contextPath;
        return rootPageURL;
    }
    
    
    public static String getCurrentUser(RoutingContext rc) {        
        User user = rc.user();
        return user!=null? user.subject(): "";
    }
    
}
