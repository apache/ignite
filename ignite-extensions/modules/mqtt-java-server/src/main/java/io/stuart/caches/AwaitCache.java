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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;

import io.stuart.caches.impl.AwaitCacheImpl;
import io.stuart.entities.cache.MqttAwaitMessage;
import io.stuart.entities.cache.MqttAwaitMessageKey;
import io.stuart.ext.collections.BoundedIgniteMap;

public interface AwaitCache {

    static AwaitCache create(Ignite ignite, CacheConfiguration<MqttAwaitMessageKey, MqttAwaitMessage> cacheCfg, CollectionConfiguration setCfg) {
        return new AwaitCacheImpl(ignite, cacheCfg, setCfg);
    }

    BoundedIgniteMap<MqttAwaitMessageKey, MqttAwaitMessage> open(String clientId);

    void close(String clientId);

    IgniteSet<MqttAwaitMessageKey> set(String clientId, boolean create);

}
