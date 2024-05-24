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

import java.util.concurrent.locks.Lock;

import io.stuart.consts.MetricsConst;
import io.stuart.log.Logger;
import io.stuart.services.cache.CacheService;
import io.stuart.services.metrics.MetricsService;
import io.stuart.services.session.SessionService;
import io.stuart.sessions.SessionWrapper;
import io.stuart.sessions.impl.PersistentSessionWrapper;
import io.stuart.sessions.impl.TransientSessionWrapper;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttEndpoint;

public class StdSessionServiceImpl extends AbstractSessionService {

    private static volatile SessionService instance;

    private StdSessionServiceImpl(Vertx vertx, CacheService cacheService) {
        super(vertx, cacheService);
    }

    public static SessionService getInstance(Vertx vertx, CacheService cacheService) {
        if (instance == null) {
            synchronized (StdSessionServiceImpl.class) {
                if (instance == null) {
                    // initialize instance
                    instance = new StdSessionServiceImpl(vertx, cacheService);
                }
            }
        }

        // return instance
        return instance;
    }

    @Override
    public void start() {
        // do nothing...
    }

    @Override
    public void stop() {
        // do nothing...
    }

    @Override
    public void openSession(MqttEndpoint endpoint) {
        // get client id
        String clientId = endpoint.clientIdentifier();
        // get clean session flag
        boolean cleanSession = endpoint.isCleanSession();
        // is persistent session
        boolean isPersistent = cacheService.isPersistentSession(clientId);

        // old session wrapper
        SessionWrapper wrapper = null;

        Logger.log().debug("clientId : {} - remote ip address is {}, port is {}", clientId, endpoint.remoteAddress().host(), endpoint.remoteAddress().port());
        Logger.log().debug("clientId : {} - clean session is {}, prev session is persistent? {}", clientId, cleanSession, isPersistent);

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
