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

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;
import java.util.function.Function;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import org.apache.commons.lang3.StringUtils;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.config.Config;
import io.stuart.consts.EventConst;
import io.stuart.consts.MetricsConst;
import io.stuart.entities.cache.MqttConnection;
import io.stuart.entities.cache.MqttListener;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.internal.MqttAuthority;
import io.stuart.entities.internal.MqttMessageTuple;
import io.stuart.entities.internal.MqttRoute;
import io.stuart.enums.Access;
import io.stuart.log.Logger;
import io.stuart.services.auth.AuthService;
import io.stuart.services.cache.CacheService;
import io.stuart.services.metrics.MetricsService;
import io.stuart.services.session.SessionService;
import io.stuart.sessions.SessionWrapper;
import io.stuart.utils.AuthUtil;
import io.stuart.utils.IdUtil;
import io.stuart.utils.MsgUtil;
import io.stuart.utils.TopicUtil;
import io.stuart.verticles.mqtt.MqttVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.mqtt.MqttAuth;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.mqtt.MqttWill;
import io.vertx.mqtt.messages.MqttPublishMessage;
import io.vertx.mqtt.messages.MqttSubscribeMessage;
import io.vertx.mqtt.messages.MqttUnsubscribeMessage;

/**
 * listener => address:port <BR/>
 * address => MqttNode.listenAddr => Config.getInstanceListenAddr() <BR/>
 * port => Config.getMqttPort()/Config.getMqttSslPort()
 */
public abstract class AbstractMqttVerticle extends AbstractVerticle implements MqttVerticle {

    protected String protocol;

    protected int port;

    protected String listener;

    protected int connMaxLimit;

    protected EventBus eventBus;

    protected CacheService cacheService;

    protected SessionService sessionService;

    protected AuthService authService;

    protected UUID thisNodeId;

    public void start() throws Exception {
        super.start();

        // set event bus
        this.eventBus = vertx.eventBus();
    }

    public void stop() throws Exception {
        // do nothing...
    }

    @Override
    public abstract MqttServerOptions initOptions();

    @Override
    public void handleEndpoint(MqttEndpoint endpoint) {
        if (limited()) {
            // reached the maximum number of connections
            handleReject(endpoint, MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE);

            // end the handler
            return;
        }

        // get client id
        String clientId = endpoint.clientIdentifier();
        // get mqtt authentication information
        MqttAuth auth = endpoint.auth();

        // client id is invalid or length > max limit
        if (!IdUtil.validateClientId(clientId) || clientId.length() > Config.getMqttClientIdMaxLen()) {
            // invalid client id
            handleReject(endpoint, MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED);
        } else if (Config.isAuthAllowAnonymous()) {
            // accept the mqtt client connect
            handleAccept(endpoint);
        } else if (!hasAuth(auth)) {
            // has not authentication information
            handleReject(endpoint, MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED);
        } else {
            Function<Boolean, Void> handler = (res) -> {
                if (res) {
                    // accept the mqtt client connect
                    handleAccept(endpoint);
                } else {
                    // bad username or password
                    handleReject(endpoint, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
                }

                // return nothing...
                return null;
            };

            if (authService != null) {
                // authentication function
                authService.auth(auth.getUsername(), auth.getPassword(), handler);
            } else {
                handler.apply(false);
            }
        }
    }

    @Override
    public void handleReject(MqttEndpoint endpoint, MqttConnectReturnCode code) {
        // reject endpoint
        endpoint.reject(code);

        // 1.metrics: mqtt 'CONNECT' received count + 1
        // 2.metrics: mqtt 'CONNACK' sent count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_CONNECT, 1, MetricsConst.PN_SM_PACKET_CONNACK, 1);
    }

    @Override
    public void handleAccept(MqttEndpoint endpoint) {
        vertx.executeBlocking(future -> {
            // initialize session
            sessionService.openSession(endpoint);

            // save mqtt connection
            saveConnection(endpoint);

            // handle the endpoint's will message
            handleWillMessage(endpoint);

            // client subscribe topics
            endpoint.subscribeHandler(subscribe -> {
                handleSubscribe(endpoint, subscribe);
            });

            // client unsubscribe topics
            endpoint.unsubscribeHandler(unsubscribe -> {
                handleUnsubscribe(endpoint, unsubscribe);
            });

            // 1.client publish message to server
            // 2.client publish 'PUBREL' to server
            endpoint.publishHandler(message -> {
                handlePublish(endpoint, message);
            }).publishReleaseHandler(messageId -> {
                handlePublishRelease(endpoint, messageId);
            });

            // 1.server receive 'PUBACK' from client
            // 2.server receive 'PUBREC' from client
            // 3.server receive 'PUBCOMP' from client
            endpoint.publishAcknowledgeHandler(messageId -> {
                handlePublishAcknowledge(endpoint, messageId);
            }).publishReceivedHandler(messageId -> {
                handlePublishReceived(endpoint, messageId);
            }).publishCompletionHandler(messageId -> {
                handlePublishCompletion(endpoint, messageId);
            });

            // client ping
            endpoint.pingHandler(v -> {
                handlePing(endpoint);
            });

            // client disconnect
            endpoint.disconnectHandler(v -> {
                handleDisconnect(endpoint);
            });

            // client disconnect or server close
            endpoint.closeHandler(v -> {
                handleClose(endpoint);
            });

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            if (endpoint.isCleanSession()) {
                endpoint.accept(false);
            } else {
                endpoint.accept(cacheService.isPersistentSession(endpoint.clientIdentifier()));
            }
        });

        // 1.metrics: mqtt 'CONNECT' received count + 1
        // 2.metrics: mqtt 'CONNACK' sent count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_CONNECT, 1, MetricsConst.PN_SM_PACKET_CONNACK, 1);
    }

    @Override
    public void handleSubscribe(MqttEndpoint endpoint, MqttSubscribeMessage subscribe) {
        // initialize 'SUBACK' result
        final List<MqttQoS> qos = new ArrayList<>();

        vertx.executeBlocking(future -> {
            // get mqtt authentication information
            MqttAuth auth = endpoint.auth();

            // username for check authority
            String checkUsername = auth == null ? null : auth.getUsername();
            // ip address for check authority
            String checkIpAddr = endpoint.remoteAddress().host();
            // client id for check authority
            String checkClientId = endpoint.clientIdentifier();

            // get mqtt session wrapper
            SessionWrapper wrapper = sessionService.getWrapper(checkClientId);

            // should check count
            int count = 0;
            // check authority list
            List<MqttAuthority> authorities = new ArrayList<>();
            // check authority
            MqttAuthority authority = null;
            // topic name
            String topicName = null;

            for (MqttTopicSubscription topicSub : subscribe.topicSubscriptions()) {
                // get topic name
                topicName = topicSub.topicName();

                // initialize authority
                authority = new MqttAuthority();
                // set topic
                authority.setTopic(topicName);

                // check topic
                if (TopicUtil.checkSubscribeTopic(topicName)) {
                    // set authority qos
                    authority.setQos(topicSub.qualityOfService().value());

                    // should check count + 1
                    ++count;
                } else {
                    // set authority qos = 0x80
                    authority.setQos(MqttQoS.FAILURE.value());
                }

                // add to authority list
                authorities.add(authority);
            }

            Function<List<MqttAuthority>, Void> handler = (checkResults) -> {
                if (checkResults == null || checkResults.isEmpty()) {
                    // complete execute blocking code
                    future.complete();
                    // handler finished
                    return null;
                }

                checkResults.forEach(checkResult -> {
                    // get topic
                    String mqttTopic = checkResult.getTopic();
                    // get qos
                    MqttQoS mqttQos = MqttQoS.valueOf(checkResult.getQos());
                    // get qos value
                    int mqttQosValue = mqttQos.value();

                    if (MqttQoS.FAILURE != mqttQos && AuthUtil.canAccess(Access.Sub, checkResult)) {
                        // add qos
                        qos.add(mqttQos);

                        // mqtt session wrapper subscribe topic
                        wrapper.subscribeTopic(mqttTopic, mqttQosValue);

                        // publish retain message
                        handlePublishRetainMessage(wrapper, mqttTopic, mqttQosValue);
                    } else {
                        // add qos
                        qos.add(MqttQoS.FAILURE);
                    }
                });

                // complete execute blocking code
                future.complete();
                // handler finished
                return null;
            };

            // check access authority, if true then send 'SUBACK' back
            if (authService != null && count > 0) {
                authService.access(checkUsername, checkIpAddr, checkClientId, authorities, handler);
            } else {
                handler.apply(authorities);
            }
        }, false, result -> {
            if (!endpoint.isSubscriptionAutoAck()) {
                // send 'SUBACK' back
                endpoint.subscribeAcknowledge(subscribe.messageId(), qos);
            }
        });

        // 1.metrics: mqtt 'SUBSCRIBE' received count + 1
        // 2.metrics: mqtt 'SUBACK' sent count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_SUBSCRIBE, 1, MetricsConst.PN_SM_PACKET_SUBACK, 1);
    }

    @Override
    public void handleUnsubscribe(MqttEndpoint endpoint, MqttUnsubscribeMessage unsubscribe) {
        vertx.executeBlocking(future -> {
            // get mqtt session wrapper
            SessionWrapper wrapper = sessionService.getWrapper(endpoint.clientIdentifier());

            if (wrapper != null) {
                unsubscribe.topics().forEach(topic -> {
                    wrapper.unsubscribeTopic(topic);
                });
            }

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            if (!endpoint.isSubscriptionAutoAck()) {
                // send 'UNSUBACK' back
                endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
            }
        });

        // 1.metrics: mqtt 'UNSUBSCRIBE' received count + 1
        // 2.metrics: mqtt 'UNSUBACK' sent count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_UNSUBSCRIBE, 1, MetricsConst.PN_SM_PACKET_UNSUBACK, 1);
    }

    @Override
    public abstract void handlePublish(MqttEndpoint endpoint, MqttPublishMessage message);

    @Override
    public abstract void handlePublishRelease(MqttEndpoint endpoint, int messageId);

    @Override
    public void handlePublishAcknowledge(MqttEndpoint endpoint, int messageId) {
        vertx.executeBlocking(future -> {
            // get mqtt session wrapper
            SessionWrapper wrapper = sessionService.getWrapper(endpoint.clientIdentifier());

            if (wrapper != null) {
                // 1.receive 'PUBACK', then delete the inflight message(QoS = 1)
                // 2.publish cached message
                wrapper.receivePuback(messageId);
            }

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            // do nothing...
        });

        // metrics: mqtt 'PUBACK' received count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBACK_RECEIVED, 1);
    }

    @Override
    public void handlePublishReceived(MqttEndpoint endpoint, int messageId) {
        vertx.executeBlocking(future -> {
            // get mqtt session wrapper
            SessionWrapper wrapper = sessionService.getWrapper(endpoint.clientIdentifier());

            if (wrapper != null) {
                // receive 'PUBREC', then update the inflight message(QoS = 2) status
                wrapper.receivePubrec(messageId);
            }

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            if (!endpoint.isPublishAutoAck()) {
                // publish 'PUBREL' to client
                endpoint.publishRelease(messageId);
            }
        });

        // 1.metrics: mqtt 'PUBREC' received count + 1
        // 2.metrics: mqtt 'PUBREL' sent count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBREC_RECEIVED, 1, MetricsConst.PN_SM_PACKET_PUBREL_SENT, 1);
    }

    @Override
    public void handlePublishCompletion(MqttEndpoint endpoint, int messageId) {
        vertx.executeBlocking(future -> {
            // get mqtt session wrapper
            SessionWrapper wrapper = sessionService.getWrapper(endpoint.clientIdentifier());

            if (wrapper != null) {
                // 1.receive 'PUBCOMP', then delete the inflight message(QoS = 2)
                // 2.publish cached message
                wrapper.receivePubcomp(messageId);
            }

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            // do nothing...
        });

        // metrics: mqtt 'PUBCOMP' received count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_PUBCOMP_RECEIVED, 1);
    }

    @Override
    public void handlePing(MqttEndpoint endpoint) {
        if (!endpoint.isAutoKeepAlive()) {
            endpoint.pong();
        }

        // 1.metrics: mqtt 'PINGREQ' received count + 1
        // 2.metrics: mqtt 'PINGRESP' sent count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_PINGREQ, 1, MetricsConst.PN_SM_PACKET_PINGRESP, 1);
    }

    @Override
    public void handleDisconnect(MqttEndpoint endpoint) {
        // metrics: mqtt 'DISCONNECT' received count + 1
        MetricsService.i().record(MetricsConst.PN_SM_PACKET_DISCONNECT, 1);
    }

    @Override
    public void handleClose(MqttEndpoint endpoint) {
        vertx.executeBlocking(future -> {
            // publish will message
            handleWillPublish(endpoint);

            // delete mqtt connection
            deleteConnection(endpoint);

            // close session
            sessionService.closeSession(endpoint);

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            // do nothing...
        });
    }

    @Override
    public void handleRetainMessage(MqttPublishMessage message) {
        if (message == null || !message.isRetain()) {
            return;
        }

        // get message payload
        byte[] payload = message.payload().getBytes();

        // check: if payload length > max payload in configuration
        if (payload != null && payload.length > Config.getMqttRetainMaxPayload()) {
            return;
        }

        if (payload == null || payload.length == 0) {
            // delete retain message
            cacheService.deleteRetain(message.topicName());

            // metrics: retain message count - 1
            MetricsService.i().record(MetricsConst.PN_SM_RETAIN_COUNT, -1);
        } else {
            // save retain message
            cacheService.saveRetain(MsgUtil.convert2MqttRetainMessage(message));

            // 1.metrics: retain message count + 1
            // 2.metrics: set retain message max
            MetricsService.i().record(MetricsConst.PN_SM_RETAIN_COUNT, 1, MetricsConst.PN_SM_RETAIN_MAX, 0);
        }
    }

    @Override
    public void handleWillMessage(MqttEndpoint endpoint) {
        // get client id
        String clientId = endpoint.clientIdentifier();
        // get will message
        MqttWill will = endpoint.will();

        if (will == null || !will.isWillFlag()) {
            return;
        }

        // get will message content
        Buffer payload = will.getWillMessage();

        if (payload == null || payload.length() == 0) {
            // delete will message
            cacheService.deleteWill(clientId, true);

            // finished
            return;
        }

        // save will message
        cacheService.saveWill(MsgUtil.convert2MqttWillMessage(will, clientId));
    }

    @Override
    public abstract void handleWillPublish(MqttEndpoint endpoint);

    @Override
    public abstract boolean limited();

    @Override
    public abstract int incrementAndGetConnCount();

    @Override
    public abstract int decrementAndGetConnCount();

    @Override
    public abstract int getConnCount();

    @Override
    public boolean hasAuth(MqttAuth auth) {
        if (auth == null) {
            return false;
        }

        String username = auth.getUsername();
        String password = auth.getPassword();

        if (StringUtils.isBlank(username) || StringUtils.isBlank(password)) {
            return false;
        }

        return true;
    }

    @Override
    public void saveListener() {
        // initialize mqtt listener
        MqttListener mqttListener = new MqttListener();

        // set mqtt listener attributes
        mqttListener.setProtocol(protocol);
        mqttListener.setAddressAndPort(listener);
        mqttListener.setConnMaxLimit(connMaxLimit);
        mqttListener.setConnCount(0);

        // save mqtt listener
        cacheService.saveListener(mqttListener);
    }

    @Override
    public void saveConnection(MqttEndpoint endpoint) {
        // get client id
        String clientId = endpoint.clientIdentifier();

        // get connection lock
        Lock lock = cacheService.getConnectionLock(clientId);

        // lock it
        lock.lock();

        try {
            if (!cacheService.containConnection(clientId)) {
                // connection count + 1
                incrementAndGetConnCount();

                // 1.metrics: node connection count + 1
                // 2.metrics: set node connection max
                MetricsService.i().grecord(MetricsConst.GPN_CONNECTION, 1);
            }

            MqttConnection conn = new MqttConnection();
            conn.setClientId(clientId);
            conn.setUsername(endpoint.auth() == null ? null : endpoint.auth().getUsername());
            conn.setIpAddr(endpoint.remoteAddress().host());
            conn.setPort(endpoint.remoteAddress().port());
            conn.setCleanSession(endpoint.isCleanSession());
            conn.setProtocolVersion(endpoint.protocolVersion());
            conn.setKeepAliveTime(endpoint.keepAliveTimeSeconds());
            conn.setConnectTime(Calendar.getInstance().getTimeInMillis());
            conn.setListener(listener);

            // save connection
            cacheService.saveConnection(conn);

            Logger.log().debug("node : {} - listen address {} after save mqtt connection, the connection count is {}", thisNodeId, listener, getConnCount());
        } finally {
            // unlock it
            lock.unlock();
        }
    }

    @Override
    public void deleteConnection(MqttEndpoint endpoint) {
        // get client id
        String clientId = endpoint.clientIdentifier();

        // get connection lock
        Lock lock = cacheService.getConnectionLock(clientId);

        // lock it
        lock.lock();

        try {
            // get session wrapper
            SessionWrapper wrapper = sessionService.getWrapper(clientId);

            // check: is the same endpoint
            if (wrapper != null && wrapper.isSameEndpoint(endpoint)) {
                // delete connection
                cacheService.deleteConnection(clientId);

                // connection count - 1
                decrementAndGetConnCount();

                // metrics: node connection count - 1
                MetricsService.i().record(MetricsConst.PN_SM_CONN_COUNT, -1);

                Logger.log().debug("node : {} - listen address {} after delete mqtt connection, the connection count is {}", thisNodeId, listener,
                        getConnCount());
            }
        } finally {
            // unlock it
            lock.unlock();
        }
    }

    @Override
    public void handlePublishRetainMessage(SessionWrapper wrapper, String topic, int qos) {
        if (wrapper == null || !TopicUtil.validateTopic(topic)) {
            return;
        }

        vertx.executeBlocking(future -> {
            // publish retain message
            wrapper.publishRetainMessage(cacheService.getRetains(topic, qos));
        }, false, result -> {
            // do nothing...
        });
    }

    @Override
    public void handleLocalPublishMessage(List<MqttRoute> routes, MqttPublishMessage message) {
        if (routes == null || routes.isEmpty() || message == null) {
            return;
        }

        // convert to mqtt message
        MqttMessage mqttMessage = MsgUtil.convert2MqttMessage(message);

        // loop routes
        routes.forEach(route -> {
            handleLocalPublishMessage(route, mqttMessage);
        });
    }

    @Override
    public void handleLocalPublishMessage(List<MqttRoute> routes, MqttMessage message) {
        if (routes == null || routes.isEmpty() || message == null) {
            return;
        }

        // loop routes
        routes.forEach(route -> {
            handleLocalPublishMessage(route, message);
        });
    }

    @Override
    public void handleLocalPublishMessage(MqttRoute route, MqttMessage message) {
        if (route == null || message == null) {
            return;
        }

        vertx.executeBlocking(future -> {
            // get client id
            String matchedClientId = route.getClientId();
            // get qos
            int matchedQos = route.getQos();

            // get mqtt session wrapper
            SessionWrapper matchedWrapper = sessionService.getWrapper(matchedClientId);

            // check: session wrapper is alive?
            if (matchedWrapper != null) {
                // publish qos2 message to client
                matchedWrapper.publishMessage(message, matchedQos);
            } else if (matchedWrapper == null && cacheService.isPersistentSession(matchedClientId)) {
                // enqueue - persistent session
                cacheService.enqueue(message, matchedClientId, matchedQos);

                // get mqtt session wrapper again
                matchedWrapper = sessionService.getWrapper(matchedClientId);

                if (matchedWrapper != null) {
                    // publish cached message
                    matchedWrapper.publishCachedMessage();
                }
            }

            // complete execute blocking code
            future.complete();
        }, false, result -> {
            // do nothing...
        });
    }

    @Override
    public void handleClusteredPublishMessage(List<MqttRoute> routes, MqttPublishMessage message) {
        if (routes == null || routes.isEmpty() || message == null) {
            return;
        }

        // convert to mqtt message
        MqttMessage mqttMessage = MsgUtil.convert2MqttMessage(message);

        // loop routes
        routes.forEach(route -> {
            handleClusteredPublishMessage(route, mqttMessage);
        });
    }

    @Override
    public void handleClusteredPublishMessage(List<MqttRoute> routes, MqttMessage message) {
        if (routes == null || routes.isEmpty() || message == null) {
            return;
        }

        // loop routes
        routes.forEach(route -> {
            handleClusteredPublishMessage(route, message);
        });
    }

    @Override
    public void handleClusteredPublishMessage(MqttRoute route, MqttMessage message) {
        if (route == null || message == null) {
            return;
        }

        // get node id
        UUID nodeId = route.getNodeId();
        // get client id
        String clientId = route.getClientId();
        // get qos
        int qos = route.getQos();

        // check: session is in local node
        if (thisNodeId.equals(nodeId)) {
            // publish message to local session
            handleLocalPublishMessage(route, message);
        } else {
            String address = EventConst.CLS_PUBLISH_TOPIC_PREFIX + nodeId.toString();
            // send mqtt message tuple to remote node
            eventBus.send(address, new MqttMessageTuple(route, message));

            eventBus.consumer(address, (Message<MqttMessageTuple> ar) -> {
                // if send failed,ec chk the session is persistent
                if (!ar.isSend() && cacheService.isPersistentSession(clientId)) {
                    vertx.executeBlocking(future -> {
                        // enqueue
                        cacheService.enqueue(message, clientId, qos);
                        // complete execute blocking code
                        future.complete();
                    }, false, result -> {
                        // do nothing...
                    });
                }
            });
        }
    }

}
