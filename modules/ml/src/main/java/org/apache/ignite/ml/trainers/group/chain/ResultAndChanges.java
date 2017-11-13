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

package org.apache.ignite.ml.trainers.group.chain;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

public class ResultAndChanges<R> {
    private R res;
    private Map<String, Map> updates = new ConcurrentHashMap<>();

    public ResultAndChanges(R res) {
        this.res = res;
    }

    public static <R> ResultAndChanges<R> of(R res) {
        return new ResultAndChanges<>(res);
    }

    public <K, V> void update(IgniteCache<K, V> cache, K key, V val) {
        String name = cache.getName();

        updates.computeIfAbsent(name, s -> new ConcurrentHashMap());
        updates.get(name).put(key, val);
    }

    void processUpdates(Ignite ignite) {
        for (Map.Entry<String, Map> entry : updates.entrySet()) {
            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(entry.getKey());

            cache.putAll(entry.getValue());
        }
    }
}
