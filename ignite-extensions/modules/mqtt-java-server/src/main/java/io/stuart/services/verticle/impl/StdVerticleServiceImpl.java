/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.services.verticle.impl;

import java.util.Timer;

import io.stuart.config.Config;
import io.stuart.log.Logger;
import io.stuart.services.auth.AuthService;
import io.stuart.services.auth.holder.AuthHolder;
import io.stuart.services.cache.CacheService;
import io.stuart.services.cache.impl.StdCacheServiceImpl;
import io.stuart.services.metrics.MetricsService;
import io.stuart.services.session.SessionService;
import io.stuart.services.session.impl.StdSessionServiceImpl;
import io.stuart.services.verticle.VerticleService;
import io.stuart.tasks.SysRuntimeInfoTask;
import io.stuart.utils.VertxUtil;
import io.stuart.verticles.mqtt.impl.StdSslMqttVerticleImpl;
import io.stuart.verticles.mqtt.impl.StdTcpMqttVerticleImpl;
import io.stuart.verticles.mqtt.impl.StdWsMqttVerticleImpl;
import io.stuart.verticles.mqtt.impl.StdWssMqttVerticleImpl;
import io.stuart.verticles.web.impl.WebVerticleImpl;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class StdVerticleServiceImpl implements VerticleService {

    private Vertx vertx;

    private CacheService cacheService;

    private SessionService sessionService;

    private AuthService authService;

    private MetricsService metricsService;

    private Timer timer = new Timer();

    @Override
    public void start() {
        Logger.log().info("Stuart's standalone instance is starting...");

        // vert.x options
        VertxOptions vertxOptions = VertxUtil.vertxOptions();
        // vertx. deployment options
        DeploymentOptions deploymentOptions = VertxUtil.vertxDeploymentOptions(vertxOptions, null);

        // get standalone cache service
        cacheService = StdCacheServiceImpl.getInstance();
        // start standalone cache service
        cacheService.start();

        // start vert.x instance
        vertx = Vertx.vertx(vertxOptions);

        // get standalone session service
        sessionService = StdSessionServiceImpl.getInstance(vertx, cacheService);
        // start standalone session service
        sessionService.start();

        // get authentication and authorization service
        authService = AuthHolder.getAuthService(vertx, cacheService);

        if (authService != null) {
            // start authentication and authorization service
            authService.start();
        }

        // create metrics service
        metricsService = MetricsService.i();
        // start metrics service
        metricsService.start();

        // deploy the standalone tcp mqtt verticle
        vertx.deployVerticle(StdTcpMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
            if (ar.succeeded()) {
                Logger.log().info("Stuart's MQTT protocol verticle(s) deploy succeeded, listen at port {}.", Config.getMqttPort());
            } else {
                Logger.log().error("Stuart's MQTT protocol verticle(s) deploy failed, excpetion: {}.", ar.cause().getMessage());
            }
        });
        // deploy the standalone websocket mqtt verticle
        vertx.deployVerticle(StdWsMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
            if (ar.succeeded()) {
                Logger.log().info("Stuart's MQTT over WebSocket verticle(s) deploy succeeded, listen at port {}.", Config.getWsPort());
            } else {
                Logger.log().error("Stuart's MQTT over WebSocket verticle(s) deploy failed, excpetion: {}.", ar.cause().getMessage());
            }
        });

        // if enable mqtt ssl protocol
        if (Config.isMqttSslEnable()) {
            // deploy the standalone ssl mqtt verticle
            vertx.deployVerticle(StdSslMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
                if (ar.succeeded()) {
                    Logger.log().info("Stuart's MQTT SSL protocol verticle(s) deploy succeeded, listen at port {}.", Config.getMqttSslPort());
                } else {
                    Logger.log().error("Stuart's MQTT SSL protocol verticle(s) deploy failed, excpetion: {}.", ar.cause().getMessage());
                }
            });
            // deploy the standalone ssl websocket mqtt verticle
            vertx.deployVerticle(StdWssMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
                if (ar.succeeded()) {
                    Logger.log().info("Stuart's MQTT over SSL WebSocket verticle(s) deploy succeeded, listen at port {}.", Config.getWssPort());
                } else {
                    Logger.log().error("Stuart's MQTT over SSL WebSocket verticle(s) deploy failed, excpetion: {}.", ar.cause().getMessage());
                }
            });
        }

        // deploy the web verticle
        vertx.deployVerticle(new WebVerticleImpl(vertx, cacheService), ar -> {
            if (ar.succeeded()) {
                Logger.log().info("Stuart's WEB management verticle deploy succeeded, listen at port {}.", Config.getHttpPort());
            } else {
                Logger.log().error("Stuart's WEB management verticle deploy failed, excpetion: {}.", ar.cause().getMessage());
            }
        });

        // set scheduled task
        timer.schedule(new SysRuntimeInfoTask(cacheService), 0, Config.getInstanceMetricsPeriodMs());
    }

    @Override
    public void stop() {
        if (vertx != null) {
            vertx.deploymentIDs().forEach(id -> {
                vertx.undeploy(id);
            });
        }

        if (metricsService != null) {
            metricsService.stop();
        }

        if (authService != null) {
            authService.stop();
        }

        if (sessionService != null) {
            sessionService.stop();
        }

        if (cacheService != null) {
            cacheService.stop();
        }
    }

}
