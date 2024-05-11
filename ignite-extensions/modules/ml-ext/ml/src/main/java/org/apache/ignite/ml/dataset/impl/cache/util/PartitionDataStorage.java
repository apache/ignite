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

package org.apache.ignite.ml.dataset.impl.cache.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Local storage used to keep partition {@code data}.
 */
class PartitionDataStorage {
    /** Storage of a partition {@code data}. */
    private final ConcurrentMap<Integer, Object> storage = new ConcurrentHashMap<>();

    /** Storage of locks correspondent to partition {@code data} objects. */
    private final ConcurrentMap<Integer, Lock> locks = new ConcurrentHashMap<>();

    /**
     * Retrieves partition {@code data} correspondent to specified partition index if it exists in local storage or
     * loads it using the specified {@code supplier}. Unlike {@link ConcurrentMap#computeIfAbsent(Object, Function)},
     * this method guarantees that function will be called only once.
     *
     * @param <D> Type of data.
     * @param part Partition index.
     * @param supplier Partition {@code data} supplier.
     * @return Partition {@code data}.
     */
    @SuppressWarnings("unchecked")
    <D> D computeDataIfAbsent(int part, Supplier<D> supplier) {
        Object data = storage.get(part);

        if (data == null) {
            Lock lock = locks.computeIfAbsent(part, p -> new ReentrantLock());

            lock.lock();
            try {
                data = storage.computeIfAbsent(part, p -> supplier.get());
            }
            finally {
                lock.unlock();
            }
        }

        return (D)data;
    }
}
