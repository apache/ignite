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

package org.apache.ignite.ml.dlearn.context.local;

import java.util.Map;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;

/**
 * Learning context partition which uses local on-heap hash map to keep data.
 */
public class LocalDLearnPartition<K, V> implements AutoCloseable {
    /** */
    private static final String PART_DATA_KEY = "part_data";

    /** */
    private final DLearnPartitionStorage storage;

    /** */
    public LocalDLearnPartition(DLearnPartitionStorage storage) {
        this.storage = storage;
    }

    /** */
    public Map<K, V> getPartData() {
        return storage.get(PART_DATA_KEY);
    }

    /** */
    public void setPartData(Map<K, V> partData) {
        storage.put(PART_DATA_KEY, partData);
    }

    /**
     * Removes all data associated with the partition.
     */
    @Override public void close() {
        storage.remove(PART_DATA_KEY);
    }
}