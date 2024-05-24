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
import java.util.concurrent.TimeUnit;

import javax.cache.Cache.Entry;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;

import io.stuart.caches.RetainCache;
import io.stuart.config.Config;
import io.stuart.entities.cache.MqttRetainMessage;
import io.stuart.functions.TopicMatchFunction;
import io.stuart.utils.QosUtil;
import io.stuart.utils.TopicUtil;

public class RetainCacheImpl implements RetainCache {

    private final ExpiryPolicy policy;

    private final IgniteCache<String, MqttRetainMessage> cache;

    public RetainCacheImpl(Ignite ignite, CacheConfiguration<String, MqttRetainMessage> cfg) {
        // check: retain expiry interval != 0
        if (Config.getMqttRetainExpiryIntervalS() > 0) {
            // initialize expiry policy
            this.policy = new CreatedExpiryPolicy(new Duration(TimeUnit.SECONDS, Config.getMqttRetainExpiryIntervalS()));
        } else {
            this.policy = null;
        }

        // get or create cache
        if (this.policy != null) {
            this.cache = ignite.getOrCreateCache(cfg).withExpiryPolicy(policy);
        } else {
            this.cache = ignite.getOrCreateCache(cfg);
        }
    }

    @Override
    public void save(MqttRetainMessage message) {
        if (message == null || !TopicUtil.validateTopic(message.getTopic())) {
            return;
        }

        if (cache.containsKey(message.getTopic())) {
            cache.put(message.getTopic(), message);
        }

        // TODO check is unsafe, if use lock, abstract mqtt verticle should use
        // vertx.executeBlocking
        if (cache.size() >= Config.getMqttRetainMaxCapacity()) {
            return;
        }

        cache.put(message.getTopic(), message);
    }

    @Override
    public boolean delete(String topic) {
        if (!TopicUtil.validateTopic(topic)) {
            return false;
        }

        return cache.remove(topic);
    }

    @Override
    public List<MqttRetainMessage> get(String topic, int qos) {
        if (!TopicUtil.validateTopic(topic)) {
            return null;
        }

        final List<MqttRetainMessage> result = new ArrayList<>();

        final ScanQuery<String, MqttRetainMessage> scan = new ScanQuery<>((key, retain) -> {
            return TopicMatchFunction.rmatch(retain.getTopic(), topic) == 1;
        });

        try (QueryCursor<Entry<String, MqttRetainMessage>> cursor = cache.query(scan)) {
            for (Entry<String, MqttRetainMessage> entry : cursor) {
                // get retain message
                MqttRetainMessage retain = entry.getValue();
                // calculate and set message qos
                retain.setQos(QosUtil.calculate(retain.getQos(), qos));
                // add to result list
                result.add(retain);
            }
        }

        return result;
    }

    @Override
    public int size() {
        return cache.size();
    }

}
