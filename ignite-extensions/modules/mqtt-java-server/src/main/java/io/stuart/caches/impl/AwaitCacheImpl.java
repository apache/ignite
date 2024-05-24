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

import java.util.concurrent.TimeUnit;

import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;

import io.stuart.caches.AwaitCache;
import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttAwaitMessageKey;
import io.stuart.ext.collections.BoundedIgniteMap;

public class AwaitCacheImpl implements AwaitCache {

    private final Ignite ignite;

    private final ExpiryPolicy policy;

    private final IgniteCache<MqttAwaitMessageKey, MqttAwaitMessage> cache;

    private final CollectionConfiguration setCfg;

    private IgnitePredicate<CacheEvent> awaitExpiredListener = evt -> {
        // get cache name
        String cacheName = evt.cacheName();

        if (CacheConst.AWAIT_MESSAGE_NAME.equals(cacheName)) {
            // get await message key
            MqttAwaitMessageKey key = ((BinaryObject) evt.key()).deserialize();
            // get await message set
            IgniteSet<MqttAwaitMessageKey> set = set(key.getClientId(), false);

            if (set != null && !set.removed()) {
                set.remove(key);
            }
        }

        // return true: continue listen
        return true;
    };

    public AwaitCacheImpl(Ignite ignite, CacheConfiguration<MqttAwaitMessageKey, MqttAwaitMessage> cacheCfg, CollectionConfiguration setCfg) {
        // set ignite instance
        this.ignite = ignite;

        // initialize created expiry policy
        this.policy = new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, Config.getSessionAwaitRelExpiryIntervalS()));

        // get or create cache
        this.cache = ignite.getOrCreateCache(cacheCfg).withExpiryPolicy(policy);

        // set ignite set collection configuration
        this.setCfg = setCfg;

        // set cache object expired event listener
        ignite.events().localListen(awaitExpiredListener, EventType.EVT_CACHE_OBJECT_EXPIRED);
    }

    @Override
    public BoundedIgniteMap<MqttAwaitMessageKey, MqttAwaitMessage> open(String clientId) {
        // get await message key's set
        IgniteSet<MqttAwaitMessageKey> set = set(clientId, true);

        // initialize and return persistent session await map
        return new BoundedIgniteMap<>(cache, set, Config.getSessionAwaitRelMaxCapacity());
    }

    @Override
    public void close(String clientId) {
        // get await message key's set
        IgniteSet<MqttAwaitMessageKey> set = set(clientId, false);

        if (set != null && !set.removed()) {
            // remove all await messages
            cache.removeAll(set);

            // close await message key's set
            set.close();
        }
    }

    @Override
    public IgniteSet<MqttAwaitMessageKey> set(String clientId, boolean create) {
        if (create) {
            return ignite.set(CacheConst.AWAIT_SET_PREFIX + clientId, setCfg);
        } else {
            return ignite.set(CacheConst.AWAIT_SET_PREFIX + clientId, null);
        }
    }

}
