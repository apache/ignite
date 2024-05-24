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
import java.util.UUID;
import java.util.concurrent.locks.Lock;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.SessionCache;
import io.stuart.consts.CacheConst;
import io.stuart.entities.cache.MqttSession;
import io.stuart.utils.IdUtil;
import io.stuart.utils.RowUtil;

public class SessionCacheImpl implements SessionCache {

    private final IgniteCache<String, MqttSession> cache;

    public SessionCacheImpl(Ignite ignite, CacheConfiguration<String, MqttSession> cfg) {
        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);
    }

    @Override
    public int save(MqttSession session) {
        if (session == null || !IdUtil.validateClientId(session.getClientId())) {
            // return error
            return CacheConst.ERROR;
        }

        // get old session
        MqttSession old = cache.get(session.getClientId());
        // put new session
        cache.put(session.getClientId(), session);

        if (session.isCleanSession() || old == null || old.isCleanSession()) {
            // return: the session is new
            return CacheConst.NEW;
        } else {
            // return: the session is old
            return CacheConst.OLD;
        }
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
    public MqttSession get(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return null;
        }

        return cache.get(clientId);
    }

    @Override
    public List<MqttSession> get(UUID nodeId, boolean cleanSession) {
        // result
        List<MqttSession> result = new ArrayList<>();
        // mqtt session
        MqttSession session = null;

        // select sql
        SqlFieldsQuery query = null;

        if (nodeId == null) {
            query = new SqlFieldsQuery("select clientId, createTime, nodeId from MqttSession where cleanSession = ?");

            try (QueryCursor<List<?>> cursor = cache.query(query.setArgs(cleanSession))) {
                for (List<?> row : cursor) {
                    session = new MqttSession();

                    session.setClientId(RowUtil.getStr(row.get(0)));
                    session.setCleanSession(cleanSession);
                    session.setCreateTime(RowUtil.getLong(row.get(1)));
                    session.setNodeId(RowUtil.getUUID(row.get(2)));

                    result.add(session);
                }
            }
        } else {
            query = new SqlFieldsQuery("select clientId, createTime from MqttSession where nodeId = ? and cleanSession = ?");

            try (QueryCursor<List<?>> cursor = cache.query(query.setArgs(nodeId, cleanSession))) {
                for (List<?> row : cursor) {
                    session = new MqttSession();

                    session.setClientId(RowUtil.getStr(row.get(0)));
                    session.setCleanSession(cleanSession);
                    session.setCreateTime(RowUtil.getLong(row.get(1)));
                    session.setNodeId(nodeId);

                    result.add(session);
                }
            }
        }

        return result;
    }

    @Override
    public Lock lock(String clientId) {
        return cache.lock(clientId);
    }

    @Override
    public int count(UUID nodeId, String clientId) {
        int count = 0;

        // select sql
        SqlFieldsQuery query = null;
        // sql arguments
        Object[] args = null;
        // client id argument
        String clientIdArg = null;

        if (!IdUtil.validateClientId(clientId)) {
            clientIdArg = "%";
        } else {
            clientIdArg = "%" + clientId + "%";
        }

        if (nodeId == null) {
            query = new SqlFieldsQuery("select count(*) from MqttSession where clientId like ?");

            args = new Object[1];
            args[0] = clientIdArg;
        } else {
            query = new SqlFieldsQuery("select count(*) from MqttSession where nodeId = ? and clientId like ?");

            args = new Object[2];
            args[0] = nodeId;
            args[1] = clientIdArg;
        }

        try (FieldsQueryCursor<List<?>> cursor = cache.query(query.setArgs(args))) {
            for (List<?> row : cursor) {
                count += RowUtil.getInt(row.get(0));
            }
        }

        return count;
    }

    @Override
    public List<MqttSession> query(UUID nodeId, String clientId, Integer pageNum, Integer pageSize) {
        // result
        List<MqttSession> result = new ArrayList<>();

        // select sql
        SqlQuery<String, MqttSession> query = null;
        // sql arguments
        Object[] args = null;

        // client id argument
        String clientIdArg = null;
        // page number argument
        Integer pageNumArg = null;
        // page size argument
        Integer pageSizeArg = null;

        if (!IdUtil.validateClientId(clientId)) {
            clientIdArg = "%";
        } else {
            clientIdArg = "%" + clientId + "%";
        }

        if (pageSize == null || pageSize <= 0) {
            pageSizeArg = CacheConst.DEF_PAGE_SIZE;
        } else {
            pageSizeArg = pageSize;
        }

        if (pageNum == null || pageNum <= 0) {
            pageNumArg = (CacheConst.DEF_PAGE_NUM - 1) * pageSizeArg;
        } else {
            pageNumArg = (pageNum - 1) * pageSizeArg;
        }

        if (nodeId == null) {
            query = new SqlQuery<>(MqttSession.class, "clientId like ? limit ? offset ?");

            args = new Object[3];
            args[0] = clientIdArg;
            args[1] = pageSizeArg;
            args[2] = pageNumArg;
        } else {
            query = new SqlQuery<>(MqttSession.class, "nodeId = ? and clientId like ? limit ? offset ?");

            args = new Object[4];
            args[0] = nodeId;
            args[1] = clientIdArg;
            args[2] = pageSizeArg;
            args[3] = pageNumArg;
        }

        try (QueryCursor<Entry<String, MqttSession>> cursor = cache.query(query.setArgs(args))) {
            for (Entry<String, MqttSession> entry : cursor) {
                // add to result
                result.add(entry.getValue());
            }
        }

        return result;
    }

}
