package io.vertx.webmvc.controller;

import io.vertx.core.Context;
import io.vertx.core.impl.ContextBase;
import io.vertx.core.impl.ContextInternal;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.VertxInstanceAware;
import io.vertx.webmvc.annotation.AuthedUser;
import io.vertx.webmvc.common.ResultDTO;

import java.util.Map.Entry;
import java.util.Set;

import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/vertx-ctx")
public class VertxContextRestController extends VertxInstanceAware{
	static final String cacheName = "__cache__";
	
	private Router subRouter;
	
	private LocalMap<String,JsonObject> cache() {
		LocalMap<String,JsonObject> cache = vertx.sharedData().getLocalMap(cacheName);
		return cache;
	}
    
    @GetMapping("/")
    public ResultDTO<JsonObject> list(RoutingContext rc) {
    	
    	JsonObject data = new JsonObject();
    	cache().forEach((k,v)->{
    		data.put(k.toString(), v);
    	});   	
    	
        return ResultDTO.success(data);
    }
    
    @GetMapping("/test/*")
    public ResultDTO<Set<String>> test(RoutingContext rc) {
    	
    	JsonObject data = JsonObject.of("headers",rc.request().headers().entries());
    	data.put("cookies", rc.request().cookieMap());
    	String key = rc.request().uri().substring("/vertx-ctx/test/".length()).replace('/', '-');
    	cache().put(key,data);
        return ResultDTO.success(data.fieldNames());
    }
    
    /**
     * 动态添加路由
     * @param path
     * @param rc
     * @return
     */
    @GetMapping("/router*")
    public ResultDTO<String> router(@RequestParam(value="path",required=false) String path,RoutingContext rc) {
    	if(path!=null && !path.isBlank()) {
    		if(subRouter==null) {
    			subRouter = Router.router(rc.vertx());    			
    		}
    		
    		if(path.equals("end")) {
    			rc.currentRoute().subRouter(subRouter);
    			return ResultDTO.success("Finshed.");
    		}
    		else if(path.charAt(0)=='/') {
    		
		    	subRouter.get(path).handler((RoutingContext rc2)->{
		    		rc2.end("this is "+path+" handler.");
		    	});
		    	
		    	return ResultDTO.success("OK");
    		}
    		else {
    			return ResultDTO.success(path);
    		}
    		
    	}
        return null;
    }
    
    @GetMapping("/:id")
    public ResultDTO<JsonObject> get(@PathVariable("id") String id,String name) {
    	
        JsonObject data = cache().get(id);
        if(data!=null) {
        	return ResultDTO.success(data);
    	}
    	return ResultDTO.failed("entity not found!");        
    }

    @AuthedUser
    @DeleteMapping("/:id")
    public ResultDTO<String> delete(@PathVariable("id") String id,@AuthedUser String author) {
    	
    	Object data = cache().remove(id);
        if(data!=null) {        	
        	return ResultDTO.success("OK");
    	}
    	return ResultDTO.failed("entity not found!");     
    }

    @AuthedUser
    @PostMapping("")
    public ResultDTO<String> save(@PathVariable("id") String id,@RequestBody JsonObject data,@AuthedUser User author) {    	
    	cache().put(id, data);
        return ResultDTO.success("OK");
    }

    @AuthedUser
    @PutMapping({"/:id","/update/:id"})
    public ResultDTO<String> update(@PathVariable("id") String id,@RequestBody JsonObject patchData,@AuthedUser User author) {
    	
    	JsonObject data = cache().get(id);
    	if(data!=null) {
    		patchData.forEach((Entry<String,Object> ent)->{
    			data.put(ent.getKey(), ent.getValue());
    		});
    		return ResultDTO.success("OK");
    	}
    	return ResultDTO.failed("entity not found!");
    }
}
