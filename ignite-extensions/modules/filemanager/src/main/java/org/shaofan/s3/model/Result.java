package org.shaofan.s3.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Result implements Serializable {

    private int code;
    private String msg;
    private Map data;
    public Result(){
        data = new HashMap();
    }

    public int getCode() {
        return code;
    }

    public Result setCode(int code) {
        this.code = code;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public Result setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public Map getData() {
        return data;
    }

    public void setData(Map data) {
        this.data = data;
    }

    public Result add(String name, Object value){
        if (data.containsKey(name)){
            data.replace(name,value);
        }else{
            data.put(name,value);
        }
        return this;
    }

    public static Result okResult(){
        Result result = new Result();
        result.code = 0;
        result.msg = "success";
        return result;
    }

    public static Result errorResult(){
        Result result = new Result();
        result.code = -1;
        result.msg = "fail";
        return result;
    }
}