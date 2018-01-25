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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.dlc.DLC;
import org.apache.ignite.ml.dlc.DLCFactory;
import org.apache.ignite.ml.dlc.DLCPartition;
import org.apache.ignite.ml.dlc.DLCPartitionRecoverableTransformer;
import org.apache.ignite.ml.dlc.DLCPartitionReplicatedTransformer;
import org.apache.ignite.ml.dlc.impl.local.util.DLCUpstreamMapAdapter;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * DLC factory which produces distributed learning contexts based on the local {@code Map}.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class MapBasedDLCFactory<K, V> implements DLCFactory<K, V> {
    /** Upstream data. */
    private final Map<K, V> upstreamData;

    /** Number of partitions. */
    private final int partitions;

    /**
     * Constructs a new instance of {@code Map} based DLC factory.
     *
     * @param upstreamData upstream data
     * @param partitions partitions
     */
    public MapBasedDLCFactory(Map<K, V> upstreamData, int partitions) {
        this.upstreamData = upstreamData;
        this.partitions = partitions;
    }

    /** {@inheritDoc} */
    @Override public <Q extends Serializable, W extends AutoCloseable, I extends DLC<K, V, Q, W>> I createDLC(
        DLCPartitionReplicatedTransformer<K, V, Q> replicatedTransformer,
        DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableTransformer,
        IgniteFunction<DLC<K, V, Q, W>, I> wrapDLC) {
        Map<Integer, DLCPartition<K, V, Q, W>> dlcMap = new HashMap<>();

        int partSize = upstreamData.size() / partitions;

        List<K> keys = new ArrayList<>(upstreamData.keySet());

        for (int partIdx = 0; partIdx < partitions; partIdx++) {
            List<K> partKeys = keys.subList(partIdx * partSize, Math.min((partIdx + 1) * partSize, upstreamData.size()));
            Q replicated = replicatedTransformer.transform(
                new DLCUpstreamMapAdapter<>(upstreamData, partKeys),
                (long) partKeys.size()
            );
            W recoverable = recoverableTransformer.transform(
                new DLCUpstreamMapAdapter<>(upstreamData, partKeys),
                (long) partKeys.size(),
                replicated
            );
            DLCPartition<K, V, Q, W> part = new DLCPartition<>(replicated, null);
            part.setRecoverableData(recoverable);
            dlcMap.put(partIdx, part);
        }

        DLC<K, V, Q, W> dlc = new MapBasedDLCImpl<>(dlcMap, partitions);

        return wrapDLC.apply(dlc);
    }
}
