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

package io.stuart.services.session.impl;

import java.util.UUID;
import java.util.concurrent.locks.Lock;

import io.stuart.consts.EventConst;
import io.stuart.consts.MetricsConst;
import io.stuart.entities.cache.MqttSession;
import io.stuart.log.Logger;
import io.stuart.services.cache.CacheService;
import io.stuart.services.metrics.MetricsService;
import io.stuart.services.session.SessionService;
import io.stuart.sessions.SessionWrapper;
import io.stuart.sessions.impl.PersistentSessionWrapper;
import io.stuart.sessions.impl.TransientSessionWrapper;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.mqtt.MqttEndpoint;

public class ClsSessionServiceImpl extends AbstractSessionService {

    private static volatile SessionService instance;

    private final EventBus eventBus;

    private final MessageConsumer<String> consumer;

    private ClsSessionServiceImpl(Vertx vertx, CacheService cacheService) {
        super(vertx, cacheService);

        // set event bus
        this.eventBus = vertx.eventBus();
        // initialize session kick consumer
        this.consumer = eventBus.consumer(EventConst.CLS_KICK_TOPIC_PREFIX + thisNodeId.toString());
    }

    public static SessionService getInstance(Vertx vertx, CacheService cacheService) {
        if (instance == null) {
            synchronized (ClsSessionServiceImpl.class) {
                if (instance == null) {
                    // initialize instance
                    instance = new ClsSessionServiceImpl(vertx, cacheService);
                }
            }
        }

        // return instance
        return instance;
    }

    @Override
    public void start() {
        // set consumer handler
        consumer.handler(message -> {
            vertx.executeBlocking(future -> {
                // get session wrapper
                SessionWrapper wrapper = getWrapper(message.body());

                if (wrapper != null) {
                    // close endpoint
                    wrapper.closeEndpoint();
                }

                // complete execute blocking code
                future.complete();
            }, false, result -> {
                // do nothing...
            });
        }).completionHandler(ar -> {
            if (ar.succeeded()) {
                Logger.log().info("Stuart's clustered session service's session kick consumer register succeeded.");
            } else {
                Logger.log().error("Stuart's clustered session service's session kick consumer register failed, exception: {}.", ar.cause().getMessage());
            }
        });
    }

    @Override
    public void stop() {
        if (consumer != null) {
            // consumer unregister
            consumer.unregister(ar -> {
                if (ar.succeeded()) {
                    Logger.log().info("Stuart's clustered session service's session kick consumer unregister succeeded.");
                } else {
                    Logger.log().error("Stuart's clustered session service's publish kick consumer unregister failed, exception: {}.", ar.cause().getMessage());
                }
            });
        }
    }

    @Override
    public void openSession(MqttEndpoint endpoint) {
        // get client id
        String clientId = endpoint.clientIdentifier();
        // get clean session flag
        boolean cleanSession = endpoint.isCleanSession();

        // get mqtt session
        MqttSession session = cacheService.getSession(clientId);
        // is persistent session
        boolean isPersistent = session == null ? false : !session.isCleanSession();
        // get previous node id
        UUID prevNodeId = session == null ? null : session.getNodeId();

        // old session wrapper
        SessionWrapper wrapper = null;

        Logger.log().debug("clientId : {} - remote ip address is {}, port is {}", clientId, endpoint.remoteAddress().host(), endpoint.remoteAddress().port());
        Logger.log().debug("clientId : {} - clean session is {}, prev session is persistent? {}", clientId, cleanSession, isPersistent);
        Logger.log().debug("node : {} - kick session, client id is {}, prev node id is {}, current node id is {}", thisNodeId, clientId, prevNodeId,
                thisNodeId);

        // get session lock
        Lock lock = cacheService.getSessionLock(clientId);
        // lock it
        lock.lock();

        try {
            // remove and get old session wrapper
            wrapper = removeWrapper(clientId);

            // save mqtt session
            saveSession(clientId, cleanSession);
        } finally {
            // unlock it
            lock.unlock();
        }

        // if it is on a remote node
        if (prevNodeId != null && prevNodeId.compareTo(thisNodeId) != 0) {
            // send kick message to target node
            eventBus.send(EventConst.CLS_KICK_TOPIC_PREFIX + prevNodeId.toString(), clientId);
        }

        // check the clean session flag
        if (cleanSession) {
            if (wrapper != null) {
                // close session wrapper
                wrapper.close();
            } else if (isPersistent) {
                // destroy persistent session
                cacheService.destroyPersistentSession(clientId);
            } else {
                // destroy transient session
                cacheService.destroyTransientSession(clientId);
            }
        } else {
            if (isPersistent && wrapper != null) {
                // close endpoint
                wrapper.closeEndpoint();
            } else if (!isPersistent && wrapper != null) {
                // close session wrapper
                wrapper.close();
            } else if (!isPersistent && wrapper == null) {
                // destroy transient session
                cacheService.destroyTransientSession(clientId);
            }
        }

        // build a new session wrapper
        if (cleanSession) {
            Logger.log().debug("clientId : {} - create a new transient session", clientId);

            // new transient session wrapper
            wrapper = new TransientSessionWrapper(vertx, cacheService, endpoint);
        } else {
            if (isPersistent && wrapper != null) {
                Logger.log().debug("clientId : {} - reuse an old persistent session", clientId);

                // refresh endpoint
                wrapper.refreshEndpoint(endpoint);
            } else {
                Logger.log().debug("clientId : {} - create a new persistent session", clientId);

                // new persistent session wrapper
                wrapper = new PersistentSessionWrapper(vertx, cacheService, endpoint);
            }
        }

        // open session wrapper
        wrapper.open();

        // put session wrapper into container
        putWrapper(clientId, wrapper);

        // 1.metrics: node session count + 1
        // 2.metrics: set node session max
        MetricsService.i().grecord(MetricsConst.GPN_SESSION, 1);
    }

}
