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

import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import io.stuart.consts.MetricsConst;
import io.stuart.entities.cache.MqttSession;
import io.stuart.log.Logger;
import io.stuart.services.cache.CacheService;
import io.stuart.services.metrics.MetricsService;
import io.stuart.services.session.SessionService;
import io.stuart.sessions.SessionWrapper;
import io.stuart.utils.IdUtil;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttEndpoint;

public abstract class AbstractSessionService implements SessionService {

    protected Vertx vertx;

    protected CacheService cacheService;

    protected UUID thisNodeId;

    protected ConcurrentHashMap<String, SessionWrapper> wrappers;

    public AbstractSessionService(Vertx vertx, CacheService cacheService) {
        this.vertx = vertx;
        this.cacheService = cacheService;
        this.thisNodeId = cacheService.localNodeId();
        this.wrappers = new ConcurrentHashMap<>();
    }

    @Override
    public abstract void start();

    @Override
    public abstract void stop();

    @Override
    public abstract void openSession(MqttEndpoint endpoint);

    @Override
    public void closeSession(MqttEndpoint endpoint) {
        // endpoint is null or closed
        if (endpoint == null || endpoint.isConnected()) {
            return;
        }

        // session wrapper is not in container
        if (!wrappers.containsKey(endpoint.clientIdentifier())) {
            return;
        }

        // get client id
        String clientId = endpoint.clientIdentifier();
        // get clean session
        boolean cleanSession = endpoint.isCleanSession();

        Logger.log().debug("node {} : close session, client id is {}, clean session is {}", thisNodeId, clientId, cleanSession);

        // get session lock
        Lock lock = cacheService.getSessionLock(clientId);

        // lock it
        lock.lock();

        try {
            // get session wrapper
            SessionWrapper wrapper = getWrapper(clientId);
            // get mqtt session
            MqttSession session = cacheService.getSession(clientId);

            // check: it is the same endpoint
            if (wrapper != null && wrapper.isSameEndpoint(endpoint)) {
                // remove session wrapper
                removeWrapper(clientId);

                // check: the session is on the same node
                if (cleanSession && session != null && thisNodeId.equals(session.getNodeId())) {
                    // destroy transient session
                    cacheService.destroyTransientSession(clientId);

                    // delete mqtt session
                    deleteSession(clientId);
                }

                // metrics: node session count - 1
                MetricsService.i().record(MetricsConst.PN_SM_SESS_COUNT, -1);
            }
        } finally {
            // unlock it
            lock.unlock();
        }

        Logger.log().debug("node : {} - after close session, the session wrapper count is {}", thisNodeId, wrappers.size());
    }

    @Override
    public void putWrapper(String clientId, SessionWrapper wrapper) {
        if (!IdUtil.validateClientId(clientId) || wrapper == null) {
            return;
        }

        wrappers.put(clientId, wrapper);
    }

    @Override
    public SessionWrapper removeWrapper(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return null;
        }

        return wrappers.remove(clientId);
    }

    @Override
    public SessionWrapper getWrapper(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return null;
        }

        return wrappers.get(clientId);
    }

    @Override
    public void saveSession(String clientId, boolean cleanSession) {
        MqttSession session = new MqttSession();

        session.setClientId(clientId);
        session.setCleanSession(cleanSession);
        session.setCreateTime(Calendar.getInstance().getTimeInMillis());
        session.setNodeId(thisNodeId);

        cacheService.saveSession(session);
    }

    @Override
    public boolean deleteSession(String clientId) {
        return cacheService.deleteSession(clientId);
    }

}
