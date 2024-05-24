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

package io.stuart.closures;

import java.util.concurrent.locks.Lock;

import org.apache.ignite.lang.IgniteClosure;

import io.stuart.entities.cache.MqttSession;
import io.stuart.services.cache.CacheService;

public class DestroySessionClosure implements IgniteClosure<MqttSession, Void> {

    private static final long serialVersionUID = 3682390996308034473L;

    private CacheService cacheService;

    public DestroySessionClosure(CacheService cacheService) {
        this.cacheService = cacheService;
    }

    @Override
    public Void apply(MqttSession session) {
        if (session == null) {
            return null;
        }

        // get client id
        String clientId = session.getClientId();
        // get session lock
        Lock lock = cacheService.getSessionLock(clientId);

        // try to lock it
        if (lock.tryLock()) {
            try {
                // get current session information
                MqttSession current = cacheService.getSession(clientId);

                if (current == null || (current.getNodeId().equals(session.getNodeId()) && current.getCreateTime() == session.getCreateTime())) {
                    if (session.isCleanSession()) {
                        // destroy transient session
                        cacheService.destroyTransientSession(clientId);
                    } else {
                        // destroy persistent session
                        cacheService.destroyPersistentSession(clientId);
                    }

                    // delete session
                    cacheService.deleteSession(clientId);
                }
            } finally {
                // unlock it
                lock.unlock();
            }
        }

        return null;
    }

}
