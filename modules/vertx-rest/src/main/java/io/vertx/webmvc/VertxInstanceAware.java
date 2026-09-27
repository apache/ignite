package io.vertx.webmvc;


import io.vertx.core.Vertx;

public abstract class VertxInstanceAware {
	protected Vertx vertx;
	protected String igniteInstanceName;	

	public void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}
	
	public void setIgniteInstanceName(String igniteInstanceName) {
		this.igniteInstanceName = igniteInstanceName;
	}
	
	public String getIgniteInstanceName() {
		return igniteInstanceName;
	}
}
