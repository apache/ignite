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

package io.stuart.verticles.mqtt.impl;

import java.util.List;
import java.util.function.Function;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.consts.MetricsConst;
import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.cache.MqttWillMessage;
import io.stuart.entities.internal.MqttAuthority;
import io.stuart.entities.internal.MqttRoute;
import io.stuart.enums.Access;
import io.stuart.log.Logger;
import io.stuart.services.auth.holder.AuthHolder;
import io.stuart.services.cache.impl.StdCacheServiceImpl;
import io.stuart.services.metrics.MetricsService;
import io.stuart.services.session.impl.StdSessionServiceImpl;
import io.stuart.sessions.SessionWrapper;
import io.stuart.utils.AuthUtil;
import io.stuart.utils.MsgUtil;
import io.stuart.utils.TopicUtil;
import io.vertx.mqtt.MqttAuth;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.messages.MqttPublishMessage;

public abstract class StdAbstractMqttVerticle extends AbstractMqttVerticle {

    @Override
    public void start() throws Exception {
        super.start();

        Logger.log().debug("Stuart's standalone mqtt verticle start...");

        // set cache service
        cacheService = StdCacheServiceImpl.getInstance();
        // set session service
        sessionService = StdSessionServiceImpl.getInstance(vertx, cacheService);
        // set authentication and authorization service
        authService = AuthHolder.getAuthService(vertx, cacheService);
        // set this node id
        thisNodeId = cacheService.localNodeId();

        // initialize mqtt server options
        MqttServerOptions options = initOptions();
        // create mqtt server
        MqttServer server = MqttServer.create(vertx, options);

        // save mqtt listener
        saveListener();

        server.endpointHandler(endpoint -> {
            handleEndpoint(endpoint);
        }).listen(ar -> {
            if (ar.succeeded()) {
                Logger.log().debug("Stuart's standalone mqtt verticle start succeeded, the verticle listen at port {}.", port);
            } else {
                Logger.log().error("Stuart's standalone mqtt verticle start failed, excpetion: {}.", ar.cause().getMessage());
            }
        });
    }

    @Override
    public void stop() throws Exception {
        super.stop();
    }

    @Override
    public abstract MqttServerOptions initOptions();

    @Override
    public void handlePublish(MqttEndpoint endpoint, MqttPublishMessage message) {
        // execute blocking code
        vertx.executeBlocking(future -> {
            // get message topic
            String topicName = message.topicName();

            // topic is not blank and should not contains '+' and '#'
            if (TopicUtil.checkPublishTopic(topicName)) {
                // get mqtt authentication information
                MqttAuth auth = endpoint.auth();

                // username for check authority
                String checkUsername = auth == null ? null : auth.getUsername();
                // ip address for check authority
                String checkIpAddr = endpoint.remoteAddress().host();
                // client id for check authority
                String checkClientId = endpoint.clientIdentifier();

                // check authority
                MqttAuthority check = new MqttAuthority();
                // set topic
                check.setTopic(topicName);

                // handler definition
                Function<MqttAuthority, Void> handler = (checkResult) -> {
                    if (!AuthUtil.canAccess(Access.Pub, checkResult)) {
                        // complete execute blocking code
                        future.complete();
                        // handler finished
                        return null;
                    }

                    // handle retain message
                    handleRetainMessage(message);

                    // get message qos
                    MqttQoS qos = message.qosLevel();

                    // check: qos == 2
                    if (qos == MqttQoS.EXACTLY_ONCE) {
                        // get this mqtt session wrapper
                        SessionWrapper wrapper = sessionService.getWrapper(endpoint.clientIdentifier());

                        if (wrapper != null) {
                            // receive and save qos2 message
                            // if await storage is full, drop it
                            wrapper.receiveQos2Message(message);
                        }
                    } else {
                        // get matched client and topic routes
                        List<MqttRoute> routes = cacheService.getRoutes(topicName, message.qosLevel().value());

                        // publish message to client
                        handleLocalPublishMessage(routes, message);
                    }

                    // complete execute blocking code
                    future.complete();
                    // handler finished
                    return null;
                };

                // check access authority, if true then publish message or save await message
                if (authService != null) {
                    authService.access(checkUsername, checkIpAddr, checkClientId, check, handler);
                } else {
                    handler.apply(check);
                }
            } else {
                // complete execute blocking code
                future.complete();
            }
        }, false, result -> {
            if (!endpoint.isPublishAutoAck()) {
                if (MqttQoS.AT_LEAST_ONCE == message.qosLevel()) {
                    // send 'PUBACK' back
                    endpoint.publishAcknowledge(message.messageId());
                } else if (MqttQoS.EXACTLY_ONCE == message.qosLevel()) {
                    // send 'PUBREC' back
                    endpoint.publishReceived(message.messageId());
                }
            }
        });

        // 1.metrics: mqtt 'PUBLISH' received count + 1
        // 2.metrics: mqtt 'PUBACK'(qos=1)/'PUBREC'(qos=2) sent count + 1
        // 3.metrics: mqtt message(qos0/1/2) received count + 1
        // 4.metrics: mqtt message received bytes count + this message length
        MetricsService.i().grecord(MetricsConst.GPN_MESSAGE_RECEIVED, message.qosLevel().value(), message.payload().getBytes().length);
    }

    @Override
    public void handlePublishRelease(MqttEndpoint endpoint, int messageId) {
        // execute blocking code
        vertx.executeBlocking(future -> {
            // get client id
            String clientId = endpoint.clientIdentifier();
            // get mqtt session wrapper
            SessionWrapper wrapper = sessionService.getWrapper(clientId);

            if (wrapper != null) {
                // get mqtt await message
                MqttAwaitMessage await = wrapper.releaseQos2Message(messageId);

                if (await != null) {
                    // get mqtt message
                    MqttMessage message = MsgUtil.convert2MqttMessage(await);
                    // get matched client and topic routes
                    List<MqttRoute> routes = cacheService.getRoutes(await.getTopic(), await.getQos());

                    // publish message to client
                    handleLocalPublishMessage(routes, message);
                }
            }

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            if (!endpoint.isPublishAutoAck()) {
                // send 'PUBCOMP' back
                endpoint.publishComplete(messageId);
            }
        });

        // 1.metrics: mqtt 'PUBREL' received count + 1
        // 2.metrics: mqtt 'PUBCOMP' sent count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBREL_RECEIVED, 1, MetricsConst.PN_SM_PACKET_PUBCOMP_SENT, 1);
    }

    @Override
    public void handleWillPublish(MqttEndpoint endpoint) {
        // get will message
        MqttWillMessage will = cacheService.getWill(endpoint.clientIdentifier());

        Logger.log().debug("node : {} - handle client id : {} close, get will message {}.", thisNodeId, endpoint.clientIdentifier(), will);

        if (will != null) {
            // get mqtt message
            MqttMessage message = MsgUtil.convert2MqttMessage(will);
            // get matched client and topic routes
            List<MqttRoute> routes = cacheService.getRoutes(will.getTopic(), will.getQos());

            // publish message to client
            handleLocalPublishMessage(routes, message);
            // delete will message
            cacheService.deleteWill(endpoint.clientIdentifier(), false);
        }
    }

    @Override
    public abstract boolean limited();

    @Override
    public abstract int incrementAndGetConnCount();

    @Override
    public abstract int decrementAndGetConnCount();

    @Override
    public abstract int getConnCount();

}
