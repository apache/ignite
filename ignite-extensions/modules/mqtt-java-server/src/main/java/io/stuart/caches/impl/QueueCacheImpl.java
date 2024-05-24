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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.configuration.CollectionConfiguration;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.stuart.caches.QueueCache;
import io.stuart.config.Config;
import io.stuart.consts.CacheConst;
import io.stuart.entities.cache.MqttMessage;
import io.stuart.ext.collections.BoundedIgniteQueue;
import io.stuart.utils.IdUtil;
import io.stuart.utils.MsgUtil;
import io.vertx.mqtt.messages.MqttPublishMessage;

public class QueueCacheImpl implements QueueCache {

    private final Ignite ignite;

    private final CollectionConfiguration cfg;

    private final int capacity;

    public QueueCacheImpl(Ignite ignite, CollectionConfiguration cfg) {
        // set ignite instance
        this.ignite = ignite;

        // set collection configuration
        this.cfg = cfg;

        // set capacity
        this.capacity = Config.getSessionQueueMaxCapacity();
    }

    @Override
    public BoundedIgniteQueue<MqttMessage> open(String clientId) {
        // get or create persistent session queue
        IgniteQueue<MqttMessage> queue = queue(clientId, true);

        // return persistent session queue wrapper
        return wrapper(queue);
    }

    @Override
    public void close(String clientId) {
        // get queue
        IgniteQueue<MqttMessage> queue = queue(clientId, false);

        if (queue != null && !queue.removed()) {
            // close queue
            queue.close();
        }
    }

    @Override
    public boolean enqueue(MqttPublishMessage message, String clientId, int qos) {
        if (message == null || !IdUtil.validateClientId(clientId)) {
            return false;
        }

        if (MqttQoS.AT_MOST_ONCE == message.qosLevel() && !Config.isSessionQueueStoreQos0()) {
            return false;
        }

        // get queue
        IgniteQueue<MqttMessage> queue = queue(clientId, false);

        if (queue == null || queue.removed()) {
            return false;
        }

        // get mqtt message
        MqttMessage mqttMessage = MsgUtil.convert2MqttMessage(message);
        // set client id
        mqttMessage.setClientId(clientId);
        // set qos
        mqttMessage.setQos(qos);

        while (true) {
            // add new item to the end
            if (queue.offer(mqttMessage)) {
                return true;
            } else {
                // poll the first item
                queue.poll();
            }
        }
    }

    @Override
    public boolean enqueue(MqttMessage message, String clientId, int qos) {
        if (message == null || !IdUtil.validateClientId(clientId)) {
            return false;
        }

        if (MqttQoS.AT_MOST_ONCE == message.publishQoS() && !Config.isSessionQueueStoreQos0()) {
            return false;
        }

        // get queue
        IgniteQueue<MqttMessage> queue = queue(clientId, false);

        if (queue == null || queue.removed()) {
            return false;
        }

        // get mqtt message
        MqttMessage mqttMessage = MsgUtil.copyMqttMessage(message);
        // set client id
        mqttMessage.setClientId(clientId);
        // set qos
        mqttMessage.setQos(qos);

        while (true) {
            // add new item to the end
            if (queue.offer(mqttMessage)) {
                return true;
            } else {
                // poll the first item
                queue.poll();
            }
        }
    }

    @Override
    public IgniteQueue<MqttMessage> queue(String clientId, boolean create) {
        // get queue name
        String name = CacheConst.QUEUE_PREFIX + clientId;

        if (create) {
            // get old queue
            IgniteQueue<MqttMessage> oq = ignite.queue(name, 0, null);
            // old queue items
            List<MqttMessage> items = null;

            if (oq != null && !oq.removed()) {
                // get old queue items
                items = oq.stream().collect(Collectors.toList());
                // close old queue
                oq.close();
            }

            // get or create new queue
            IgniteQueue<MqttMessage> nq = ignite.queue(name, capacity, cfg);

            if (items != null && !items.isEmpty()) {
                // get items' size
                int size = items.size();

                if (capacity > 0 && size > capacity) {
                    nq.addAll(items.subList(size - capacity, size));
                } else {
                    nq.addAll(items);
                }
            }

            return nq;
        } else {
            return ignite.queue(name, 0, null);
        }
    }

    @Override
    public BoundedIgniteQueue<MqttMessage> wrapper(IgniteQueue<MqttMessage> queue) {
        return new BoundedIgniteQueue<>(queue);
    }

}
