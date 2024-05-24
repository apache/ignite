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

package io.stuart.caches.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.WillCache;
import io.stuart.entities.cache.MqttWillMessage;
import io.stuart.utils.IdUtil;

public class WillCacheImpl implements WillCache {

    private final IgniteCache<String, MqttWillMessage> cache;

    public WillCacheImpl(Ignite ignite, CacheConfiguration<String, MqttWillMessage> cfg) {
        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);
    }

    @Override
    public void save(MqttWillMessage message) {
        if (message == null || !IdUtil.validateClientId(message.getClientId())) {
            return;
        }

        cache.put(message.getClientId(), message);
    }

    @Override
    public boolean delete(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return false;
        }

        return cache.remove(clientId);
    }

    @Override
    public MqttWillMessage get(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return null;
        }

        return cache.get(clientId);
    }

}
