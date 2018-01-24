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

package org.apache.ignite.ml.dlc.impl.cache.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 * Partition recoverable data storage is a data structure maintained locally on the node and used to keep recoverable
 * part of partitions.
 *
 * @param <W> type of recoverable data
 */
public class DLCPartitionRecoverableDataStorage<W extends AutoCloseable> {
    /** Map with partitions index as a key and recoverable data as a value. */
    private final ConcurrentMap<Integer, W> data = new ConcurrentHashMap<>();

    /** Map with partition index as a key and lock as a value. */
    private final ConcurrentMap<Integer, Lock> locks = new ConcurrentHashMap<>();

    /** */
    public ConcurrentMap<Integer, W> getData() {
        return data;
    }

    /** */
    public ConcurrentMap<Integer, Lock> getLocks() {
        return locks;
    }
}
