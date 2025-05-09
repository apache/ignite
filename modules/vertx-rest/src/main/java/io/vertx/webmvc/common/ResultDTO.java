package io.vertx.webmvc.common;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.Serializable;

/**
 * The result object returned by the request
 *
 * @author zbw
 */
@Getter
@Setter
@ToString
public class ResultDTO<T> implements Serializable {
    
    private int code;   // 200 is success 
    private String message;
    private T data;
    

    public static <T> ResultDTO<T> success(T data) {
        ResultDTO<T> r = new ResultDTO<>();       
        r.data = data;
        r.code = 200;
        return r;
    }

    public static <T> ResultDTO<T> failed(String message) {
        ResultDTO<T> r = new ResultDTO<>();        
        r.message = message;
        r.code = 500;
        return r;
    }

    public static <T> ResultDTO<T> anyThing(T data, String message, int code) {
        ResultDTO<T> r = new ResultDTO<>();
        r.message = message;
        r.code = code;
        r.data = data;
        return r;
    }

    public static <T> ResultDTO<T> failed(Throwable t) {
        return failed(ExceptionUtils.getStackTrace(t));
    }

}
