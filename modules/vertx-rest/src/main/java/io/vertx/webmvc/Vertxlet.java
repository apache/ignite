package io.vertx.webmvc;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;


public abstract class Vertxlet implements Handler<RoutingContext> {

    
    public <T> void init(Promise<T> promise) {
        try {
            init();
            promise.complete();
        } catch (Exception e) {
            promise.fail(e);
        }
    }

    
    public <T> void destroy(Promise<T> promise) {
        try {
            destroy();
            promise.complete();
        } catch (Exception e) {
            promise.fail(e);
        }
    }

    @Override
    public void handle(RoutingContext rc) {
        try {
            switch (rc.request().method().name()) {
                case "GET":
                    doGet(rc.request(),rc.response());
                    break;
                case "POST":
                    doPost(rc.request(),rc.response());
                    break;
                case "PUT":
                    doPut(rc.request(),rc.response());
                    break;
                case "HEAD":
                    doHead(rc.request(),rc.response());
                    break;
                case "DELETE":
                    doDelete(rc.request(),rc.response());
                    break;
                case "OPTIONS":
                    doOptions(rc.request(),rc.response());
                    break;
                default:
                    throw new IllegalArgumentException("HTTP method: "
                            + rc.request().method() + " is not supported");
            }
        } catch (Exception e) {
            rc.fail(e);
        }
    }

    protected void init() throws Exception {
    }

    protected void destroy() throws Exception {
    }

    protected void doGet(HttpServerRequest request, HttpServerResponse response) throws Exception {
        response.end();
    }

    protected void doPost(HttpServerRequest request, HttpServerResponse response) throws Exception {
    	response.end();
    }

    protected void doPut(HttpServerRequest request, HttpServerResponse response) throws Exception {
    	response.end();
    }

    protected void doHead(HttpServerRequest request, HttpServerResponse response) throws Exception {
    	response.end();
    }

    protected void doOptions(HttpServerRequest request, HttpServerResponse response) throws Exception {
    	response.setStatusCode(200);
    	response.end();
    }
    
    protected void doDelete(HttpServerRequest request, HttpServerResponse response) throws Exception {
    	response.end();
    }
}