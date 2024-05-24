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

import java.util.ArrayList;
import java.util.List;

import javax.cache.Cache.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.ListenerCache;
import io.stuart.entities.cache.MqttListener;

public class ListenerCacheImpl implements ListenerCache {

    private final IgniteCache<String, MqttListener> cache;

    public ListenerCacheImpl(Ignite ignite, CacheConfiguration<String, MqttListener> cfg) {
        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);
    }

    @Override
    public boolean saveIfAbsent(MqttListener listener) {
        if (listener == null || StringUtils.isBlank(listener.getAddressAndPort())) {
            return false;
        }

        return cache.putIfAbsent(listener.getAddressAndPort(), listener);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public List<MqttListener> query() {
        List<MqttListener> result = new ArrayList<>();

        SqlQuery<String, MqttListener> query = new SqlQuery<>(MqttListener.class, "order by protocol, addressAndPort");

        try (QueryCursor<Entry<String, MqttListener>> cursor = cache.query(query)) {
            for (Entry<String, MqttListener> entry : cursor) {
                result.add(entry.getValue());
            }
        }

        return result;
    }

}
