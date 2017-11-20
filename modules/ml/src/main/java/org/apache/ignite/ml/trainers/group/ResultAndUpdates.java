/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.trainers.group;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

public class ResultAndUpdates<R> {
    private R res;
    private Map<String, Map> updates = new ConcurrentHashMap<>();

    public ResultAndUpdates(R res) {
        this.res = res;
    }

    ResultAndUpdates(R res, Map<String, Map> updates) {
        this.res = res;
        this.updates = updates;
    }

    public static <R> ResultAndUpdates<R> empty() {
        return new ResultAndUpdates<>(null);
    }

    public static <R> ResultAndUpdates<R> of(R res) {
        return new ResultAndUpdates<>(res);
    }

    public <K, V> void update(IgniteCache<K, V> cache, K key, V val) {
        String name = cache.getName();

        updates.computeIfAbsent(name, s -> new ConcurrentHashMap());
        updates.get(name).put(key, val);
    }

    public R result() {
        return res;
    }

    static <R> ResultAndUpdates<R> sum(IgniteBinaryOperator<R> op, R identity, Collection<ResultAndUpdates<R>> resultsAndUpdates) {
        Map<String, Map> allUpdates = new HashMap<>();

        for (ResultAndUpdates<R> ru : resultsAndUpdates) {
            for (String cacheName : ru.updates.keySet()) {
                allUpdates.computeIfAbsent(cacheName, s -> new HashMap());

                allUpdates.get(cacheName).putAll(ru.updates.get(cacheName));
            }
        }

        R res = resultsAndUpdates.stream().map(ResultAndUpdates::result).reduce(op).orElse(identity);

        return new ResultAndUpdates<>(res, allUpdates);
    }

    void processUpdates(Ignite ignite) {
        for (Map.Entry<String, Map> entry : updates.entrySet()) {
            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(entry.getKey());

            cache.putAll(entry.getValue());
        }
    }
}
