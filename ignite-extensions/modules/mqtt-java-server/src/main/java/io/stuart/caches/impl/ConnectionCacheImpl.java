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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.ConnectionCache;
import io.stuart.consts.CacheConst;
import io.stuart.entities.cache.MqttConnection;
import io.stuart.utils.IdUtil;
import io.stuart.utils.RowUtil;

public class ConnectionCacheImpl implements ConnectionCache {

    private final IgniteCache<String, MqttConnection> cache;

    public ConnectionCacheImpl(Ignite ignite, CacheConfiguration<String, MqttConnection> cfg) {
        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);
    }

    @Override
    public void save(MqttConnection conn) {
        if (conn == null || !IdUtil.validateClientId(conn.getClientId())) {
            return;
        }

        cache.put(conn.getClientId(), conn);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public boolean delete(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return false;
        }

        return cache.remove(clientId);
    }

    @Override
    public boolean contains(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return false;
        }

        return cache.containsKey(clientId);
    }

    @Override
    public MqttConnection get(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return null;
        }

        return cache.get(clientId);
    }

    @Override
    public Lock lock(String clientId) {
        return cache.lock(clientId);
    }

    @Override
    public Map<String, Integer> count() {
        Map<String, Integer> result = new HashMap<>();

        SqlFieldsQuery query = new SqlFieldsQuery("select listener, count(*) from MqttConnection group by listener");

        try (FieldsQueryCursor<List<?>> cursor = cache.query(query)) {
            for (List<?> row : cursor) {
                result.put(RowUtil.getStr(row.get(0)), RowUtil.getInt(row.get(1)));
            }
        }

        return result;
    }

    @Override
    public int count(String clientId) {
        int count = 0;

        SqlFieldsQuery query = new SqlFieldsQuery("select count(*) from MqttConnection where clientId like ?");

        String arg = null;

        if (!IdUtil.validateClientId(clientId)) {
            arg = "%";
        } else {
            arg = "%" + clientId + "%";
        }

        try (FieldsQueryCursor<List<?>> cursor = cache.query(query.setArgs(arg))) {
            for (List<?> row : cursor) {
                count += RowUtil.getInt(row.get(0));
            }
        }

        return count;
    }

    @Override
    public List<MqttConnection> query(String clientId, Integer pageNum, Integer pageSize) {
        // result
        List<MqttConnection> result = new ArrayList<>();

        SqlQuery<String, MqttConnection> query = new SqlQuery<>(MqttConnection.class, "clientId like ? order by connectTime limit ? offset ?");

        Object[] args = new Object[3];
        Integer ps = null;

        if (!IdUtil.validateClientId(clientId)) {
            args[0] = "%";
        } else {
            args[0] = "%" + clientId + "%";
        }

        if (pageSize == null || pageSize <= 0) {
            ps = CacheConst.DEF_PAGE_SIZE;
        } else {
            ps = pageSize;
        }

        args[1] = ps;

        if (pageNum == null || pageNum <= 0) {
            args[2] = (CacheConst.DEF_PAGE_NUM - 1) * ps;
        } else {
            args[2] = (pageNum - 1) * ps;
        }

        try (QueryCursor<Entry<String, MqttConnection>> cursor = cache.query(query.setArgs(args))) {
            for (Entry<String, MqttConnection> entry : cursor) {
                result.add(entry.getValue());
            }
        }

        return result;
    }

}
