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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.cache.Cache.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.transactions.Transaction;

import io.stuart.caches.RouterCache;
import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.consts.TopicConst;
import io.stuart.entities.cache.MqttRouter;
import io.stuart.entities.cache.MqttRouterKey;
import io.stuart.entities.cache.MqttTrie;
import io.stuart.entities.cache.MqttTrieKey;
import io.stuart.entities.internal.MqttRoute;
import io.stuart.functions.QosCalcFunction;
import io.stuart.utils.IdUtil;
import io.stuart.utils.RowUtil;
import io.stuart.utils.TopicUtil;

public class RouterCacheImpl implements RouterCache {

    private static final String[] POUND = new String[] { TopicConst.POUND };

    private final Ignite ignite;

    private final IgniteCache<MqttRouterKey, MqttRouter> routerCache;

    private final IgniteCache<MqttTrieKey, MqttTrie> trieCache;

    public RouterCacheImpl(Ignite ignite, CacheConfiguration<MqttRouterKey, MqttRouter> routerCfg, CacheConfiguration<MqttTrieKey, MqttTrie> trieCfg) {
        // set ignite
        this.ignite = ignite;

        // set qos calculate sql function
        routerCfg.setSqlFunctionClasses(QosCalcFunction.class);

        // get or create cache
        this.routerCache = ignite.getOrCreateCache(routerCfg);

        // get or create cache
        this.trieCache = ignite.getOrCreateCache(trieCfg);
    }

    @Override
    public void save(MqttRouter router) {
        if (router == null || !IdUtil.validateClientId(router.getClientId()) || !TopicUtil.validateTopic(router.getTopic())) {
            return;
        }

        // get router key
        MqttRouterKey key = new MqttRouterKey(router.getClientId(), router.getTopic());
        // get topic
        String topic = router.getTopic();

        // topic trie nodes
        List<MqttTrie> nodes = null;
        // trie node key
        MqttTrieKey trieKey = null;
        // trie node
        MqttTrie trieNode = null;

        // check: topic is wildcard
        if (TopicUtil.isWildcard(topic)) {
            // get topic trie nodes
            nodes = TopicUtil.topic2Trie(topic);
        }

        IgniteTransactions transactions = ignite.transactions();

        try (Transaction tx = transactions.txStart()) {
            // get old router
            MqttRouter oldRouter = routerCache.get(key);

            // router is existed
            if (oldRouter != null) {
                // set new node id
                oldRouter.setNodeId(router.getNodeId());
                // set new qos
                oldRouter.setQos(router.getQos());

                // update router
                routerCache.put(key, oldRouter);
            } else {
                // add new router
                routerCache.put(key, router);

                if (nodes != null && !nodes.isEmpty()) {
                    // loop this topic's all trie node
                    for (MqttTrie node : nodes) {
                        // get trie node key
                        trieKey = new MqttTrieKey(node.getParent(), node.getWord());
                        // get trie node
                        trieNode = trieCache.get(trieKey);

                        // if trie node is not existed
                        if (trieNode == null) {
                            // add new trie node
                            trieCache.put(trieKey, node);
                        } else {
                            // set trie node count + 1
                            trieNode.setCount(trieNode.getCount() + 1);
                            // update trie node
                            trieCache.put(trieKey, trieNode);
                        }
                    }
                }
            }

            tx.commit();
        }
    }

    @Override
    public void save(MqttRouter router, UUID nodeId) {
        if (router == null || nodeId == null) {
            return;
        }

        // set node id
        router.setNodeId(nodeId);

        // save router
        save(router);
    }

    @Override
    public void delete(String clientId) {
        if (!IdUtil.validateClientId(clientId)) {
            return;
        }

        SqlFieldsQuery sql = new SqlFieldsQuery("select topic from MqttRouter where clientId = ?");

        try (QueryCursor<List<?>> cursor = routerCache.query(sql.setArgs(clientId))) {
            for (List<?> row : cursor) {
                delete(clientId, RowUtil.getStr(row.get(0)));
            }
        }
    }

    @Override
    public void delete(String clientId, String topic) {
        if (!IdUtil.validateClientId(clientId) || !TopicUtil.validateTopic(topic)) {
            return;
        }

        // get router key
        MqttRouterKey key = new MqttRouterKey(clientId, topic);
        // topic trie nodes
        List<MqttTrie> nodes = null;

        // trie node key
        MqttTrieKey trieKey = null;
        // trie node
        MqttTrie trieNode = null;
        // trie node's count
        int count = 0;

        // check: topic is wildcard
        if (TopicUtil.isWildcard(topic)) {
            // get topic trie nodes
            nodes = TopicUtil.topic2Trie(topic);
        }

        IgniteTransactions transactions = ignite.transactions();

        try (Transaction tx = transactions.txStart()) {
            // remove router
            if (routerCache.remove(key) && nodes != null && !nodes.isEmpty()) {
                // loop this topic's all trie node
                for (int i = nodes.size() - 1; i >= 0; --i) {
                    // get trie node key
                    trieKey = new MqttTrieKey(nodes.get(i).getParent(), nodes.get(i).getWord());
                    // get trie node
                    trieNode = trieCache.get(trieKey);

                    if (trieNode != null) {
                        // get trie node's count
                        count = trieNode.getCount();

                        if (count > 1) {
                            // trie node count - 1
                            trieNode.setCount(count - 1);
                            // update trie node
                            trieCache.put(trieKey, trieNode);
                        } else {
                            // remove trie node
                            trieCache.remove(trieKey);
                        }
                    }
                }
            }

            tx.commit();
        }
    }

    @Override
    public void update(String clientId, UUID nodeId) {
        // update sql
        SqlFieldsQuery query = new SqlFieldsQuery("update MqttRouter set nodeId = ? where clientId = ?");

        // execute sql
        routerCache.query(query.setArgs(nodeId, clientId));
    }

    @Override
    public MqttRouter get(String clientId, String topic) {
        if (!IdUtil.validateClientId(clientId) || !TopicUtil.validateTopic(topic)) {
            return null;
        }

        // get router key
        MqttRouterKey key = new MqttRouterKey(clientId, topic);

        // get and return mqtt router
        return routerCache.get(key);
    }

    @Override
    public List<MqttRoute> getRoutes(String topic, int qos) {
        // result
        List<MqttRoute> result = new ArrayList<>();
        // route
        MqttRoute route = null;

        // get matched topics
        Set<String> matches = getMatches(topic);

        if (matches == null || matches.isEmpty()) {
            return result;
        }

        // initialize query arguments
        List<Object> args = new ArrayList<>(matches);
        // add qos argument
        args.add(0, qos);

        // placeholder(?) array
        String[] placeholders = new String[matches.size()];
        // fill placeholder to ?
        Arrays.fill(placeholders, "?");

        StringBuilder sql = new StringBuilder();

        if (Config.isSessionUpgradeQos()) {
            sql.append(" select clientId, upgrade(?, qos) from MqttRouter where topic in ( ");
        } else {
            sql.append(" select clientId, downgrade(?, qos) from MqttRouter where topic in ( ");
        }

        sql.append(StringUtils.join(placeholders, ","));
        sql.append(" ) ");

        SqlFieldsQuery query = new SqlFieldsQuery(sql.toString());

        try (QueryCursor<List<?>> cursor = routerCache.query(query.setArgs(args.toArray()))) {
            for (List<?> row : cursor) {
                route = new MqttRoute();

                route.setClientId(RowUtil.getStr(row.get(0)));
                route.setQos(RowUtil.getInt(row.get(1)));

                result.add(route);
            }
        }

        return result;
    }

    @Override
    public List<MqttRoute> getClusteredRoutes(String topic, int qos) {
        // result
        List<MqttRoute> result = new ArrayList<>();
        // route
        MqttRoute route = null;

        // get matched topics
        Set<String> matches = getMatches(topic);

        if (matches == null || matches.isEmpty()) {
            return result;
        }

        // initialize query arguments
        List<Object> args = new ArrayList<>(matches);
        // add qos argument
        args.add(0, qos);

        // placeholder(?) array
        String[] placeholders = new String[matches.size()];
        // fill placeholder to ?
        Arrays.fill(placeholders, "?");

        // select local transient and clustered persistent sessions sql
        StringBuilder sql = new StringBuilder();

        if (Config.isSessionUpgradeQos()) {
            sql.append(" select nodeId, clientId, upgrade(?, qos) from MqttRouter where topic in ( ");
        } else {
            sql.append(" select nodeId, clientId, downgrade(?, qos) from MqttRouter where topic in ( ");
        }

        sql.append(StringUtils.join(placeholders, ","));
        sql.append(" )");

        SqlFieldsQuery query = new SqlFieldsQuery(sql.toString());

        try (QueryCursor<List<?>> cursor = routerCache.query(query.setArgs(args.toArray()))) {
            for (List<?> row : cursor) {
                route = new MqttRoute();

                route.setNodeId(RowUtil.getUUID(row.get(0)));
                route.setClientId(RowUtil.getStr(row.get(1)));
                route.setQos(RowUtil.getInt(row.get(2)));

                result.add(route);
            }
        }

        return result;
    }

    @Override
    public int countTopics(UUID nodeId, String topic) {
        int count = 0;

        // select sql
        SqlFieldsQuery query = null;
        // sql arguments
        Object[] args = null;
        // topic argument
        String topicArg = null;

        if (!TopicUtil.validateTopic(topic)) {
            topicArg = "%";
        } else {
            topicArg = "%" + topic + "%";
        }

        if (nodeId == null) {
            query = new SqlFieldsQuery("select count(*) from (select distinct nodeId, topic from MqttRouter where topic like ?) c");

            args = new Object[1];
            args[0] = topicArg;
        } else {
            query = new SqlFieldsQuery("select count(*) from (select distinct nodeId, topic from MqttRouter where topic like ? and nodeId = ?) c");

            args = new Object[2];
            args[0] = topicArg;
            args[1] = nodeId;
        }

        try (FieldsQueryCursor<List<?>> cursor = routerCache.query(query.setArgs(args))) {
            for (List<?> row : cursor) {
                count += RowUtil.getInt(row.get(0));
            }
        }

        return count;
    }

    @Override
    public List<MqttRouter> queryTopics(UUID nodeId, String topic, Integer pageNum, Integer pageSize) {
        // result
        List<MqttRouter> result = new ArrayList<>();
        // router
        MqttRouter item = null;

        // select sql
        SqlFieldsQuery query = null;
        // sql arguments
        Object[] args = null;

        // topic argument
        String topicArg = null;
        // page number argument
        Integer pageNumArg = null;
        // page size argument
        Integer pageSizeArg = null;

        if (!TopicUtil.validateTopic(topic)) {
            topicArg = "%";
        } else {
            topicArg = "%" + topic + "%";
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
            query = new SqlFieldsQuery("select distinct nodeId, topic from MqttRouter where topic like ? limit ? offset ?");

            args = new Object[3];
            args[0] = topicArg;
            args[1] = pageSizeArg;
            args[2] = pageNumArg;
        } else {
            query = new SqlFieldsQuery("select distinct nodeId, topic from MqttRouter where topic like ? and nodeId = ? limit ? offset ?");

            args = new Object[4];
            args[0] = topicArg;
            args[1] = nodeId;
            args[2] = pageSizeArg;
            args[3] = pageNumArg;
        }

        try (FieldsQueryCursor<List<?>> cursor = routerCache.query(query.setArgs(args))) {
            for (List<?> row : cursor) {
                item = new MqttRouter();

                item.setNodeId(RowUtil.getUUID(row.get(0)));
                item.setTopic(RowUtil.getStr(row.get(1)));

                result.add(item);
            }
        }

        return result;
    }

    @Override
    public int countSubscribes(UUID nodeId, String clientId) {
        int count = 0;

        // select sql
        SqlFieldsQuery query = null;
        // sql arguments
        Object[] args = null;
        // client id argument
        String cidArg = null;

        if (!IdUtil.validateClientId(clientId)) {
            cidArg = "%";
        } else {
            cidArg = "%" + clientId + "%";
        }

        if (nodeId == null) {
            query = new SqlFieldsQuery("select count(*) from MqttRouter where clientId like ?");

            args = new Object[1];
            args[0] = cidArg;
        } else {
            query = new SqlFieldsQuery("select count(*) from MqttRouter where clientId like ? and nodeId = ?");

            args = new Object[2];
            args[0] = cidArg;
            args[1] = nodeId;
        }

        try (FieldsQueryCursor<List<?>> cursor = routerCache.query(query.setArgs(args))) {
            for (List<?> row : cursor) {
                count += RowUtil.getInt(row.get(0));
            }
        }

        return count;
    }

    @Override
    public List<MqttRouter> querySubscribes(UUID nodeId, String clientId, Integer pageNum, Integer pageSize) {
        List<MqttRouter> result = new ArrayList<>();

        // select sql
        SqlQuery<String, MqttRouter> query = null;
        // sql arguments
        Object[] args = null;

        // topic argument
        String cidArg = null;
        // page number argument
        Integer pageNumArg = null;
        // page size argument
        Integer pageSizeArg = null;

        if (!IdUtil.validateClientId(clientId)) {
            cidArg = "%";
        } else {
            cidArg = "%" + clientId + "%";
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
            query = new SqlQuery<>(MqttRouter.class, "clientId like ? limit ? offset ?");

            args = new Object[3];
            args[0] = cidArg;
            args[1] = pageSizeArg;
            args[2] = pageNumArg;
        } else {
            query = new SqlQuery<>(MqttRouter.class, "clientId like ? and nodeId = ? limit ? offset ?");

            args = new Object[4];
            args[0] = cidArg;
            args[1] = nodeId;
            args[2] = pageSizeArg;
            args[3] = pageNumArg;
        }

        try (QueryCursor<Entry<String, MqttRouter>> cursor = routerCache.query(query.setArgs(args))) {
            for (Entry<String, MqttRouter> entry : cursor) {
                result.add(entry.getValue());
            }
        }

        return result;
    }

    private Set<String> getMatches(String topic) {
        if (!TopicUtil.validateTopic(topic)) {
            return null;
        }

        // matched topics
        final Set<String> matches = new HashSet<>();
        // add topic itself
        matches.add(topic);
        // query other matched topics
        queryMatches(null, TopicUtil.words(topic), matches);

        // return
        return matches;
    }

    private void queryMatches(String parent, String[] words, final Set<String> matches) {
        // get word
        String word = words[0];
        // check has more word
        boolean left = (words.length - 1) > 0;
        // get matched nodes
        List<MqttTrie> nodes = trie(parent, word);

        for (MqttTrie node : nodes) {
            if (TopicConst.POUND.equals(word)) {
                if (TopicConst.POUND.equals(node.getWord())) {
                    // add to matches' set
                    matches.add(node.getSelf());
                }

                // next loop
                continue;
            }

            if (TopicConst.POUND.equals(node.getWord())) {
                // add to matches' set
                matches.add(node.getSelf());
                // next loop
                continue;
            }

            if (left) {
                // query next word
                queryMatches(node.getSelf(), Arrays.copyOfRange(words, 1, words.length), matches);
            } else {
                // add to matches' set
                matches.add(node.getSelf());
                // query last '#' word
                queryMatches(node.getSelf(), POUND, matches);
            }
        }
    }

    private List<MqttTrie> trie(String parent, String word) {
        // result nodes
        final List<MqttTrie> nodes = new ArrayList<>();
        // get keys
        Set<MqttTrieKey> keys = new HashSet<>();
        // set keys
        keys.add(new MqttTrieKey(parent, TopicConst.PLUS));
        keys.add(new MqttTrieKey(parent, TopicConst.POUND));
        keys.add(new MqttTrieKey(parent, word));

        Map<MqttTrieKey, MqttTrie> result = trieCache.getAll(keys);

        if (result != null && !result.isEmpty()) {
            result.forEach((key, value) -> {
                nodes.add(value);
            });
        }

        return nodes;
    }

}
