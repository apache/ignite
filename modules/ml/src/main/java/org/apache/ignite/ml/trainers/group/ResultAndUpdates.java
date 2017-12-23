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

/**
 * Class containing result of computation and updates which should be made for caches.
 * Purpose of this class is mainly performance optimization: suppose we have multiple computations which run in parallel
 * and do some updates to caches. It is more efficient to collect all changes from all this computations and perform them
 * in batch.
 *
 * @param <R> Type of computation result.
 */
public class ResultAndUpdates<R> {
    /**
     * Result of computation.
     */
    private R res;

    /**
     * Updates in the form cache name -> (key -> new value).
     */
    private Map<String, Map> updates = new ConcurrentHashMap<>();

    /**
     * Construct an instance of this class.
     *
     * @param res Computation result.
     */
    public ResultAndUpdates(R res) {
        this.res = res;
    }

    /**
     * Construct an instance of this class.
     *
     * @param res Computation result.
     * @param updates Map of updates in the form cache name -> (key -> new value).
     */
    ResultAndUpdates(R res, Map<String, Map> updates) {
        this.res = res;
        this.updates = updates;
    }

    /**
     * Construct an empty result.
     *
     * @param <R> Result type.
     * @return Empty result.
     */
    public static <R> ResultAndUpdates<R> empty() {
        return new ResultAndUpdates<>(null);
    }

    /**
     * Construct {@link ResultAndUpdates} object from given result.
     *
     * @param res Result of computation.
     * @param <R> Type of result of computation.
     * @return ResultAndUpdates object.
     */
    public static <R> ResultAndUpdates<R> of(R res) {
        return new ResultAndUpdates<>(res);
    }

    /**
     * Add a cache update to this object.
     *
     * @param cache Cache to be updated.
     * @param key Key of cache to be updated.
     * @param val New value.
     * @param <K> Type of key of cache to be updated.
     * @param <V> New value.
     */
    public <K, V> void update(IgniteCache<K, V> cache, K key, V val) {
        String name = cache.getName();

        updates.computeIfAbsent(name, s -> new ConcurrentHashMap());
        updates.get(name).put(key, val);
    }

    /**
     * Get result of computation.
     *
     * @return Result of computation.
     */
    public R result() {
        return res;
    }

    /**
     * Sum collection of ResultAndUpdate into one: results are reduced by specified binary operator and updates are merged.
     *
     * @param op Binary operator used to combine computation results.
     * @param identity Identity for op.
     * @param resultsAndUpdates ResultAndUpdates to be combined with.
     * @param <R> Type of computation result.
     * @return Sum of collection ResultAndUpdate objects.
     */
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

    /**
     * Get updates map.
     *
     * @return Updates map.
     */
    public Map<String, Map> updates() {
        return updates;
    }

    /**
     * Set updates map.
     *
     * @param updates New updates map.
     * @return This object.
     */
    ResultAndUpdates<R> setUpdates(Map<String, Map> updates) {
        this.updates = updates;
        return this;
    }

    /**
     * Apply updates to caches.
     *
     * @param ignite Ignite instance.
     */
    void applyUpdates(Ignite ignite) {
        for (Map.Entry<String, Map> entry : updates.entrySet()) {
            IgniteCache<Object, Object> cache = ignite.getOrCreateCache(entry.getKey());

            cache.putAll(entry.getValue());
        }
    }
}
