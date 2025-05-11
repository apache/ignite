package org.apache.ignite.console.agent.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;

public class HttpGetTop extends AbstractVerticle {
	
	@Override
    public void start(){
		HttpClient client = vertx.createHttpClient();
	    Future<HttpClientRequest> req = client.request(
	      HttpMethod.GET,
	      18080,
	      "localhost",
	      "/ignite?cmd=top");
	    
	    Future<HttpClientResponse> $resp = req.result().send();
	    
	    $resp.andThen(r -> {
	    	HttpClientResponse resp = r.result();
	    	int status = resp.statusCode();
		    Buffer body = resp.body().result();
		    
	    });	    

    }
	
	public static void main(String[] args) {
        VertxOptions options = new VertxOptions();
        Vertx.clusteredVertx(options,res->{
            if(res.succeeded()) {
                Vertx vertx = res.result();
                vertx.deployVerticle("org.apache.ignite.console.agent.verticle.HttpGetTop");
            }else{
                //失败的时候做什么！
            }
        });
    }
}
