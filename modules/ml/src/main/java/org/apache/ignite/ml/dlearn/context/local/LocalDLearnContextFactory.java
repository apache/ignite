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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.ml.dlearn.DLearnContextFactory;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.utils.DLearnContextPartitionKey;

/**
 * Factory produces local learning context by extracting data from local list of values.
 *
 * @param <V> type of upstream values
 */
public class LocalDLearnContextFactory<V> implements DLearnContextFactory<LocalDLearnPartition<V>> {
    /** */
    private static final long serialVersionUID = -7614441997952907675L;

    /** */
    private final List<V> data;

    /** */
    private final int partitions;

    /** */
    public LocalDLearnContextFactory(List<V> data, int partitions) {
        this.data = data;
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @Override public LocalDLearnContext<LocalDLearnPartition<V>> createContext() {
        Map<DLearnContextPartitionKey, Object> learningCtxMap = new HashMap<>();
        UUID learningCtxId = UUID.randomUUID();

        int partSize = data.size() / partitions;

        // loads data into learning context partitions
        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            List<V> partData = new ArrayList<>();
            for (int j = partIdx * partSize; j < (partIdx + 1) * partSize && j < data.size(); j++)
                partData.add(data.get(j));

            DLearnPartitionStorage storage = new LocalDLearnPartitionStorage(learningCtxMap, learningCtxId, partIdx);
            LocalDLearnPartition<V> part = new LocalDLearnPartition<>(storage);
            part.setPartData(partData);
        }

        return new LocalDLearnContext<>(learningCtxMap, LocalDLearnPartition::new, learningCtxId, partitions);
    }
}