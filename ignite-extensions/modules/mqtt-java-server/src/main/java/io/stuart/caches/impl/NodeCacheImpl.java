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

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;

import io.stuart.caches.NodeCache;
import io.stuart.entities.cache.MqttNode;
import io.stuart.enums.Status;

public class NodeCacheImpl implements NodeCache {

    private final Ignite ignite;

    private final IgniteCache<UUID, MqttNode> cache;

    public NodeCacheImpl(Ignite ignite, CacheConfiguration<UUID, MqttNode> cfg) {
        // set ignite
        this.ignite = ignite;

        // get or create cache
        this.cache = ignite.getOrCreateCache(cfg);
    }

    @Override
    public boolean saveIfAbsent(MqttNode node) {
        if (node == null || node.getNodeId() == null) {
            return false;
        }

        return cache.putIfAbsent(node.getNodeId(), node);
    }

    @Override
    public void clear() {
        cache.clear();
    }

    @Override
    public boolean delete(UUID nodeId) {
        if (nodeId == null) {
            return false;
        }

        return cache.remove(nodeId);
    }

    @Override
    public void update(MqttNode node) {
        if (node == null || node.getNodeId() == null) {
            return;
        }

        // get node id
        UUID nodeId = node.getNodeId();

        IgniteTransactions transactions = ignite.transactions();

        try (Transaction tx = transactions.txStart()) {
            // get mqtt node
            MqttNode mqttNode = cache.get(nodeId);

            if (mqttNode != null) {
                mqttNode.setThread(node.getThread());
                mqttNode.setCpu(node.getCpu());
                mqttNode.setHeap(node.getHeap());
                mqttNode.setOffHeap(node.getOffHeap());
                mqttNode.setMaxFileDescriptors(node.getMaxFileDescriptors());

                cache.put(nodeId, mqttNode);
            }

            tx.commit();
        }
    }

    @Override
    public void update(UUID nodeId, Status status) {
        if (nodeId == null || status == null) {
            return;
        }

        // get node
        MqttNode node = cache.get(nodeId);

        if (node != null) {
            // set node status
            node.setStatus(status.value());

            // update node
            cache.put(nodeId, node);
        }
    }

    @Override
    public boolean contains(UUID nodeId) {
        if (nodeId == null) {
            return false;
        }

        return cache.containsKey(nodeId);
    }

    @Override
    public MqttNode get(UUID nodeId) {
        if (nodeId == null) {
            return null;
        }

        return cache.get(nodeId);
    }

    @Override
    public MqttNode get(String instanceId, String listenAddr) {
        if (StringUtils.isBlank(instanceId) || StringUtils.isBlank(listenAddr)) {
            return null;
        }

        // result
        MqttNode result = null;

        // select sql
        SqlQuery<UUID, MqttNode> query = new SqlQuery<>(MqttNode.class, "instanceId = ? and listenAddr = ?");

        try (QueryCursor<Entry<UUID, MqttNode>> cursor = cache.query(query.setArgs(instanceId, listenAddr))) {
            for (Entry<UUID, MqttNode> entry : cursor) {
                // get mqtt node
                result = entry.getValue();

                // break;
                break;
            }
        }

        // return result
        return result;
    }

    @Override
    public Lock lock(UUID nodeId) {
        if (nodeId == null) {
            return null;
        }

        MqttNode node = cache.get(nodeId);

        if (node == null || Status.Stopped.value() == node.getStatus()) {
            return null;
        }

        return cache.lock(nodeId);
    }

    @Override
    public List<MqttNode> query(Status status) {
        // result
        List<MqttNode> result = new ArrayList<>();

        // select sql
        SqlQuery<UUID, MqttNode> query = null;
        // arguments
        Object[] args = null;

        if (status == null) {
            // initialize sql
            query = new SqlQuery<>(MqttNode.class, "order by instanceId, listenAddr");
            // initialize arguments
            args = new Object[0];
        } else {
            // initialize sql
            query = new SqlQuery<>(MqttNode.class, "status = ? order by instanceId, listenAddr");
            // initialize arguments
            args = new Object[] { status.value() };
        }

        try (QueryCursor<Entry<UUID, MqttNode>> cursor = cache.query(query.setArgs(args))) {
            for (Entry<UUID, MqttNode> entry : cursor) {
                result.add(entry.getValue());
            }
        }

        // return result
        return result;
    }

}
