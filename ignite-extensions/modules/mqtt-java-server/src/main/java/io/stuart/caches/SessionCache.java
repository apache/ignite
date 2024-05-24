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

package io.stuart.caches;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.impl.SessionCacheImpl;
import io.stuart.entities.cache.MqttSession;

public interface SessionCache {

    static SessionCache create(Ignite ignite, CacheConfiguration<String, MqttSession> cfg) {
        return new SessionCacheImpl(ignite, cfg);
    }

    int save(MqttSession session);

    boolean delete(String clientId);

    boolean contains(String clientId);

    MqttSession get(String clientId);

    List<MqttSession> get(UUID nodeId, boolean cleanSession);

    Lock lock(String clientId);

    int count(UUID nodeId, String clientId);

    List<MqttSession> query(UUID nodeId, String clientId, Integer pageNum, Integer pageSize);

}
