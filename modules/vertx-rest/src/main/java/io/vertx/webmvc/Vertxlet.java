package io.vertx.webmvc;

import java.io.IOException;
import java.util.Locale;


import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.webmvc.common.VertxletException;
import io.vertx.webmvc.common.VertxHttpServerResponseOutputStream;
import io.vertx.webmvc.common.WebConstant;

public abstract class Vertxlet implements Handler<RoutingContext>, ApplicationContextAware, java.io.Serializable {

	private ApplicationContext springContext;

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) {
		this.springContext = applicationContext;
	}

	protected String getInitParameter(String name){
		ApplicationContext sc = springContext;
		if (sc == null) {
			throw new IllegalStateException("err.servlet_config_not_initialized");
		}

		return sc.getEnvironment().getProperty(name);
	}

	protected void setContentType(HttpServerResponse response, String contentType, String encoding) {
		response.putHeader("Content-Type", contentType + ";charset=" + encoding);
	}

	protected void setContentType(HttpServerResponse response, String contentType) {
		response.putHeader("Content-Type", contentType);
	}

	protected VertxHttpServerResponseOutputStream getOutputStream(HttpServerResponse response) {
		return new VertxHttpServerResponseOutputStream(response);
	}

	public <T> void init(Promise<T> promise) {
		try {
			init();
			promise.complete();
		} catch (VertxletException e) {
			promise.fail(e);
		}
	}

	public <T> void destroy(Promise<T> promise) {
		try {
			destroy();
			promise.complete();
		} catch (VertxletException e) {
			promise.fail(e);
		}
	}

	@Override
	public void handle(RoutingContext rc) {
		try {
			service(rc, rc.request(), rc.response());
		} 
		catch (VertxletException e) {
			rc.response().setStatusCode(e.getHttpStatus());
			rc.fail(e);
		}
		catch (IOException e) {
			rc.fail(e);
		}
	}

	public void service(RoutingContext rc, HttpServerRequest request, HttpServerResponse response) throws VertxletException,IOException {

		switch (request.method().name()) {
			case "GET":
				doGet(request, response);
				break;
			case "POST":
				doPost(rc);
				break;
			case "PUT":
				doPut(rc);
				break;
			case "HEAD":
				doHead(request, response);
				break;
			case "DELETE":
				doDelete(request, response);
				break;
			case "OPTIONS":
				doOptions(request, response);
				break;
			default:
				throw new VertxletException(501,"HTTP method: " + request.method() + " is not supported");
		}

	}

	protected void init() throws VertxletException {
	}

	protected void destroy() throws VertxletException {
	}

	protected void doGet(HttpServerRequest request, HttpServerResponse response) throws VertxletException, IOException {
		response.end();
	}
	
	protected void doPost(RoutingContext rc) throws VertxletException, IOException {
		doPost(rc.request(),rc.response());
	}

	protected void doPost(HttpServerRequest request, HttpServerResponse response) throws VertxletException, IOException {
		response.end();
	}
	
	protected void doPut(RoutingContext rc) throws VertxletException, IOException {
		doPut(rc.request(),rc.response());
	}

	protected void doPut(HttpServerRequest request, HttpServerResponse response) throws VertxletException, IOException {
		response.end();
	}

	protected void doHead(HttpServerRequest request, HttpServerResponse response) throws VertxletException, IOException {
		response.end();
	}

	protected void doOptions(HttpServerRequest request, HttpServerResponse response) throws VertxletException, IOException {
		response.setStatusCode(200);
		response.end();
	}

	protected void doDelete(HttpServerRequest request, HttpServerResponse response) throws VertxletException, IOException {
		response.end();
	}

	public static final boolean isMultipartContent(HttpServerRequest request) {
		String contentType = request.getHeader("Content-Type");
		if (contentType == null) {
			return false;
		}
		if (contentType.toLowerCase(Locale.ENGLISH).startsWith(WebConstant.MULTIPART)) {
			return true;
		}
		return false;
	}
}