package io.vertx.webmvc.starter;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.webmvc.VertxInstanceAware;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.plugin.IgniteVertxPlugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * vert.x starter
 *
 */
@Slf4j
public class VertXStarter extends VertxInstanceAware {

    @Data
    public static class State {
        boolean deployed;
        boolean started;
        boolean failed;
        Throwable cause;
    }

    private List<AbstractVerticle> webApps = new ArrayList<>();
    private List<State>  webAppsState = new ArrayList<>();

    private static Map<String,VertXStarter> instanceMap = new ConcurrentHashMap<>();

    public static VertXStarter getInstance(String gridName){
        VertXStarter instance = instanceMap.get(gridName);
        if(instance==null){
            instance = new VertXStarter(gridName);
            instanceMap.put(gridName,instance);
        }
        return instance;
    }

    private VertXStarter(String gridName) {
        this.igniteInstanceName = gridName;
    }

    public State insertVerticle(int i, AbstractVerticle webApp) {
        State state = new State();
        this.webAppsState.add(i,state);
        this.webApps.add(i,webApp);
        return state;
	}

	public State addVerticle(AbstractVerticle webApp) {
        State state = new State();
        this.webAppsState.add(state);
		this.webApps.add(webApp);
        return state;
	}


    public void start() {
        long startwatch = System.currentTimeMillis();
        log.info("[Vertx web] Vertx web's vert.x system start to bootstrap...");
        Ignite ignite = Ignition.ignite(igniteInstanceName);
        IgniteVertxPlugin plugin = ignite.plugin("Vertx");
        vertx = plugin.vertx();

        for(int i=0;i<webApps.size();i++) {
            final State state = webAppsState.get(i);
            if(state.deployed){
                continue;
            }

            log.info("[Vertx web] Begin deploy verticle {}.",webApps.get(i).getClass().getName());
	        vertx.deployVerticle(webApps.get(i)).andThen((s)->{
                state.deployed=true;
                if(s.succeeded()){
                    state.started=true;
                    log.info("[Vertx web] Deploy verticle successfully.{}",s.result());
                }
                else{
                    state.failed = s.failed();
                    state.cause = s.cause();
                    log.error("[Vertx web] Failed deploy verticle {}.",state.cause.getMessage());
                }
            });
        }

        long stopwatch = System.currentTimeMillis();
        log.info("[Vertx web] Vertx web's vert.x system started successfully, using time {}s.", (stopwatch-startwatch)/1000);
    }

    public void stop() {
        long startwatch = System.currentTimeMillis();
        log.info("[Vertx web] Vertx web's vert.x system start to stop...");
        Ignite ignite = Ignition.ignite(igniteInstanceName);
        IgniteVertxPlugin plugin = ignite.plugin("Vertx");
        vertx = plugin.vertx();

        for(int i=0;i<webApps.size();i++) {
            final State state = webAppsState.get(i);
            if(!state.deployed){
                continue;
            }
            
            vertx.undeploy(webApps.get(i).getClass().getName()).andThen((s)->{
                if(s.succeeded()){
                    state.deployed=false;
                    state.started=false;
                    log.info("[Vertx web] Undeploy verticle successfully.{}",s.result());
                }
                else{
                    state.failed = s.failed();
                    state.cause = s.cause();
                    log.error("[Vertx web] Failed to undeploy verticle {}.",state.cause.getMessage());
                }
            });
        }
        long stopwatch = System.currentTimeMillis();
        log.info("[Vertx web] Vertx web's vert.x system stoped, using time {}s.", (stopwatch-startwatch)/1000);
    }


}
