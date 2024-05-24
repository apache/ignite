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
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;

import io.stuart.caches.InflightCache;
import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.entities.cache.MqttMessageKey;
import io.stuart.ext.collections.BoundedIgniteMapUnsafe;

public class InflightCacheImpl implements InflightCache {

    private final Ignite ignite;

    private final IgniteCache<MqttMessageKey, MqttMessage> cache;

    private final CollectionConfiguration setCfg;

    public InflightCacheImpl(Ignite ignite, CacheConfiguration<MqttMessageKey, MqttMessage> cacheCfg, CollectionConfiguration setCfg) {
        // set ignite
        this.ignite = ignite;

        // get or create cache
        this.cache = ignite.getOrCreateCache(cacheCfg);

        // set ignite set collection configuration
        this.setCfg = setCfg;
    }

    @Override
    public BoundedIgniteMapUnsafe<MqttMessageKey, MqttMessage> open(String clientId) {
        // get inflight message key's set
        IgniteSet<MqttMessageKey> set = set(clientId, true);

        // initialize and return persistent session inflight map
        return new BoundedIgniteMapUnsafe<>(cache, set, Config.getSessionInflightMaxCapacity());
    }

    @Override
    public void close(String clientId) {
        // get inflight message key's set
        IgniteSet<MqttMessageKey> set = set(clientId, false);

        if (set != null && !set.removed()) {
            // remove all inflight messages
            cache.removeAll(set);

            // close inflight message key's set
            set.close();
        }
    }

    @Override
    public IgniteSet<MqttMessageKey> set(String clientId, boolean create) {
        if (create) {
            return ignite.set(CacheConst.INFLIGHT_SET_PREFIX + clientId, setCfg);
        } else {
            return ignite.set(CacheConst.INFLIGHT_SET_PREFIX + clientId, null);
        }
    }

}
