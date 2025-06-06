package org.apache.ignite.internal.plugin;

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.plugin.IgnitePlugin;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class IgniteVertxPlugin implements IgnitePlugin {
	Vertx vertx;
	private VertxOptions options;
	private IgniteLogger log;
	volatile boolean finish = false;
	
	public IgniteVertxPlugin(VertxOptions options, IgniteLogger log) {
		this.options = options;
		this.log = log;
	}
	
	public Vertx getVertx() {
		return vertx;
	}

	public synchronized Vertx vertx() {
		
		if (vertx == null) {
			try {
				long startwatch = System.currentTimeMillis();
				finish = false;
				Vertx.clusteredVertx(options, res -> {
					if (res.succeeded()) {
						vertx = res.result();
						finish = true;

						long stopwatch = System.currentTimeMillis();
						long spend = (stopwatch - startwatch) / 1000;
						log.info("[Vertx web] Vertx web's vert.x system started successfully, using time {}s.",spend+"");

					} else {
						// 失败的时候做什么！
						log.error("Failt to start Vertx cluster.", res.cause());
					}
				});

			} catch (Exception e) {
				log.error("[Vertx web] create vertx fail.", e);
				finish = true;
			}
		}
		
		while(!finish) {
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return vertx;
	}
}
