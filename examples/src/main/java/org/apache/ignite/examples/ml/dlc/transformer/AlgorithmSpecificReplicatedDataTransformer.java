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

package org.apache.ignite.examples.ml.dlc.transformer;

import org.apache.ignite.ml.dlc.DLCPartitionReplicatedTransformer;
import org.apache.ignite.ml.dlc.DLCUpstreamEntry;

/**
 * Transformer which transforms upstream data into algorithm-specific replicated data.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public class AlgorithmSpecificReplicatedDataTransformer<K, V> implements DLCPartitionReplicatedTransformer<K, V, AlgorithmSpecificReplicatedData> {
    /** */
    private static final long serialVersionUID = -3720585607267757357L;

    /** {@inheritDoc} */
    @Override public AlgorithmSpecificReplicatedData transform(Iterable<DLCUpstreamEntry<K, V>> upstreamData,
        Long upstreamDataSize) {
        return new AlgorithmSpecificReplicatedData();
    }
}
