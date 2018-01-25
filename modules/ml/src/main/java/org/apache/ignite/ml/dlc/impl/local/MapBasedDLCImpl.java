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

package org.apache.ignite.ml.dlc.impl.local;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.ml.dlc.DLC;
import org.apache.ignite.ml.dlc.DLCPartition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.jetbrains.annotations.NotNull;

/**
 * Local Map based implementation of a Distributed Learning Context.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 * @param <Q> type of replicated data of a partition
 * @param <W> type of recoverable data of a partition
 */
public class MapBasedDLCImpl<K, V, Q extends Serializable, W extends AutoCloseable>
    implements DLC<K, V, Q, W> {
    /** Map containing pairs of partition index and partitions. */
    private final Map<Integer, DLCPartition<K, V, Q, W>> dlcMap;

    /** Number of partitions. */
    private final int partitions;

    /**
     * Constructs a new instance of a local Map based Distributed Learning Context.
     *
     * @param dlcMap distributed learning context map
     * @param partitions number of partitions
     */
    public MapBasedDLCImpl(Map<Integer, DLCPartition<K, V, Q, W>> dlcMap, int partitions) {
        this.dlcMap = dlcMap;
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<DLCPartition<K, V, Q, W>, Integer, R> mapper, IgniteBinaryOperator<R> reducer,
        R identity) {

        R res = identity;

        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            DLCPartition<K, V, Q, W> part = dlcMap.get(partIdx);

            R partRes = mapper.apply(part, partIdx);

            res = reducer.apply(res, partRes);

            dlcMap.put(partIdx, part);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        dlcMap.clear();
    }

    private class MapBasedCacheEntry implements Cache.Entry<K, V> {

        private final K key;

        private final V value;

        public MapBasedCacheEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        @Override public K getKey() {
            return key;
        }

        @Override public V getValue() {
            return value;
        }

        @Override public <T> T unwrap(Class<T> clazz) {
            throw new UnsupportedOperationException();
        }
    }

    private class MapBasedIterable implements Iterable<Cache.Entry<K, V>> {

        private final Iterator<Cache.Entry<K, V>> iterator;

        public MapBasedIterable(Iterator<Cache.Entry<K, V>> iterator) {
            this.iterator = iterator;
        }

        @NotNull @Override public Iterator<Cache.Entry<K, V>> iterator() {
            return iterator;
        }
    }
}