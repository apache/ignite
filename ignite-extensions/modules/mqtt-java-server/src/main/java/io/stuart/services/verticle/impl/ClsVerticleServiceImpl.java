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
import io.stuart.entities.internal.MqttMessageTuple;
import io.stuart.entities.internal.codec.MqttMessageTupleCodec;
import io.stuart.exceptions.StartException;
import io.stuart.log.Logger;
import io.stuart.services.auth.AuthService;
import io.stuart.services.auth.holder.AuthHolder;
import io.stuart.services.cache.CacheService;
import io.stuart.services.cache.impl.ClsCacheServiceImpl;
import io.stuart.services.metrics.MetricsService;
import io.stuart.services.session.SessionService;
import io.stuart.services.session.impl.ClsSessionServiceImpl;
import io.stuart.services.verticle.VerticleService;
import io.stuart.tasks.SysRuntimeInfoTask;
import io.stuart.utils.VertxUtil;
import io.stuart.verticles.mqtt.impl.ClsSslMqttVerticleImpl;
import io.stuart.verticles.mqtt.impl.ClsTcpMqttVerticleImpl;
import io.stuart.verticles.mqtt.impl.ClsWsMqttVerticleImpl;
import io.stuart.verticles.mqtt.impl.ClsWssMqttVerticleImpl;
import io.stuart.verticles.web.impl.WebVerticleImpl;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.spi.cluster.ClusterManager;
import io.vertx.spi.cluster.ignite.IgniteClusterManager;

public class ClsVerticleServiceImpl implements VerticleService {

    private Vertx vertx;

    private CacheService cacheService;

    private SessionService sessionService;

    private AuthService authService;

    private MetricsService metricsService;

    private Timer timer = new Timer();

    @Override
    public void start() {
        Logger.log().info("Stuart's clustered instance is starting...");

        // get vert.x options
        VertxOptions vertxOptions = VertxUtil.vertxOptions();
        // get vertx. deployment options
        DeploymentOptions deploymentOptions = VertxUtil.vertxDeploymentOptions(vertxOptions, null);


        // get clustered cache service
        cacheService = ClsCacheServiceImpl.getInstance();
        // start clustered cache service
        cacheService.start();

        // get vert.x cluster manager
        IgniteClusterManager clusterManager = ((ClsCacheServiceImpl)cacheService).getClusterManager();
        // set vert.x cluster manager
        vertxOptions.setClusterManager(clusterManager);

        Vertx.clusteredVertx(vertxOptions, result -> {
            if (result.succeeded()) {
                // set vert.x instance
                vertx = result.result();
                // set mqtt message tuple codec
                vertx.eventBus().registerDefaultCodec(MqttMessageTuple.class, new MqttMessageTupleCodec());

                // get clustered session service
                sessionService = ClsSessionServiceImpl.getInstance(vertx, cacheService);
                // start clustered session service
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

                // deploy the clustered tcp mqtt verticle
                vertx.deployVerticle(ClsTcpMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
                    if (ar.succeeded()) {
                        Logger.log().info("Stuart's MQTT protocol verticle(s) deploy succeeded, listen at port {}.", Config.getMqttPort());
                    } else {
                        Logger.log().error("Stuart's MQTT protocol verticle(s) deploy failed, excpetion: {}.", ar.cause().getMessage());
                    }
                });
                // deploy the clustered websocket mqtt verticle
                vertx.deployVerticle(ClsWsMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
                    if (ar.succeeded()) {
                        Logger.log().info("Stuart's MQTT over WebSocket verticle(s) deploy succeeded, listen at port {}.", Config.getWsPort());
                    } else {
                        Logger.log().error("Stuart's MQTT over WebSocket verticle(s) deploy failed, excpetion: {}.", ar.cause().getMessage());
                    }
                });

                // if enable mqtt ssl protocol
                if (Config.isMqttSslEnable()) {
                    // deploy the clustered ssl mqtt verticle
                    vertx.deployVerticle(ClsSslMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
                        if (ar.succeeded()) {
                            Logger.log().info("Stuart's MQTT SSL protocol verticle(s) deploy succeeded, listen at port {}.", Config.getMqttSslPort());
                        } else {
                            Logger.log().error("Stuart's MQTT SSL protocol verticle(s) deploy failed, excpetion: {}.", ar.cause().getMessage());
                        }
                    });
                    // deploy the clustered ssl websocket mqtt verticle
                    vertx.deployVerticle(ClsWssMqttVerticleImpl.class.getName(), deploymentOptions, ar -> {
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
            } else {
                Logger.log().error("Stuart's clustered vert.x instance start failed, exception: {}.", result.cause());

                throw new StartException(result.cause());
            }
        });
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
