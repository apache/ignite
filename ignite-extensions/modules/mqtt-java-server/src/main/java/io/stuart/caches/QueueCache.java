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
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.configuration.CollectionConfiguration;

import io.stuart.caches.impl.QueueCacheImpl;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.ext.collections.BoundedIgniteQueue;
import io.vertx.mqtt.messages.MqttPublishMessage;

public interface QueueCache {

    static QueueCache create(Ignite ignite, CollectionConfiguration cfg) {
        return new QueueCacheImpl(ignite, cfg);
    }

    BoundedIgniteQueue<MqttMessage> open(String clientId);

    void close(String clientId);

    boolean enqueue(MqttPublishMessage message, String clientId, int qos);

    boolean enqueue(MqttMessage message, String clientId, int qos);

    IgniteQueue<MqttMessage> queue(String clientId, boolean create);

    BoundedIgniteQueue<MqttMessage> wrapper(IgniteQueue<MqttMessage> queue);

}
