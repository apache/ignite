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
import java.util.UUID;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;

/**
 * D-learn partition storage based on on-heap hash map for local processing and tests. Doesn't require Ignite cluster
 * to work.
 */
public class LocalDLearnPartitionStorage implements DLearnPartitionStorage {
    /** Learning context physical storage. */
    private final Map<DLearnContextPartitionKey, Object> learningCtxMap;

    /** Learning context id. */
    private final UUID learningCtxId;

    /** Partition index. */
    private final int part;

    /**
     * Constructs a new instance of local learning partition storage.
     *
     * @param learningCtxMap learning context physical storage
     * @param learningCtxId learning context id
     * @param part partition index
     */
    public LocalDLearnPartitionStorage(
        Map<DLearnContextPartitionKey, Object> learningCtxMap, UUID learningCtxId, int part) {
        this.learningCtxMap = learningCtxMap;
        this.learningCtxId = learningCtxId;
        this.part = part;
    }

    /** {@inheritDoc} */
    @Override public <T> void put(String key, T val) {
        learningCtxMap.put(new DLearnContextPartitionKey(part, learningCtxId, key), val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T get(String key) {
        return (T)learningCtxMap.get(new DLearnContextPartitionKey(part, learningCtxId, key));
    }

    /** {@inheritDoc} */
    @Override public void remove(String key) {
        learningCtxMap.remove(new DLearnContextPartitionKey(part, learningCtxId, key));
    }
}
