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
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.impl.WillCacheImpl;
import io.stuart.entities.cache.MqttWillMessage;

public interface WillCache {

    static WillCache create(Ignite ignite, CacheConfiguration<String, MqttWillMessage> cfg) {
        return new WillCacheImpl(ignite, cfg);
    }

    void save(MqttWillMessage message);

    boolean delete(String clientId);

    MqttWillMessage get(String clientId);

}
