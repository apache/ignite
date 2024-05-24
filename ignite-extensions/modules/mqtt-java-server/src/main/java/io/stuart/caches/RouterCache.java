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

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.impl.RouterCacheImpl;
import io.stuart.entities.cache.MqttRouter;
import io.stuart.entities.cache.MqttRouterKey;
import io.stuart.entities.cache.MqttTrie;
import io.stuart.entities.cache.MqttTrieKey;
import io.stuart.entities.internal.MqttRoute;

public interface RouterCache {

    static RouterCache create(Ignite ignite, CacheConfiguration<MqttRouterKey, MqttRouter> routerCfg, CacheConfiguration<MqttTrieKey, MqttTrie> trieCfg) {
        return new RouterCacheImpl(ignite, routerCfg, trieCfg);
    }

    void save(MqttRouter router);

    void save(MqttRouter router, UUID nodeId);

    void delete(String clientId);

    void delete(String clientId, String topic);

    void update(String clientId, UUID nodeId);

    MqttRouter get(String clientId, String topic);

    List<MqttRoute> getRoutes(String topic, int qos);

    List<MqttRoute> getClusteredRoutes(String topic, int qos);

    int countTopics(UUID nodeId, String topic);

    List<MqttRouter> queryTopics(UUID nodeId, String topic, Integer pageNum, Integer pageSize);

    int countSubscribes(UUID nodeId, String clientId);

    List<MqttRouter> querySubscribes(UUID nodeId, String clientId, Integer pageNum, Integer pageSize);

}
