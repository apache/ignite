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

package org.apache.ignite.ml.dlc;

import java.io.Serializable;

/**
 * Distributed Learning Context partition which consists of replicated data and recoverable data. Replicated
 * data is stored in a reliable storage (Ignite Cache) and in case of the node failure or rebalancing automatically
 * restored on another node. Recoverable data is stored in a non-reliable local storage and in case of node failure or
 * rebalancing when partition is restored on another node should be reloaded from the upstream.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 * @param <Q> type of replicated data of a partition
 * @param <W> type of recoverable data of a partition
 */
public class DLCPartition<K, V, Q extends Serializable, W extends AutoCloseable> implements Serializable {
    /** */
    private static final long serialVersionUID = -6348461866724022880L;

    /** Replicated data. */
    private final Q replicatedData;

    /** Loader of the recoverable part of this partition. */
    private final DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableDataTransformer;

    /** Recoverable data. */
    private transient W recoverableData;

    /**
     * Constructs a new instance of a DLC partition.
     *
     * @param replicatedData replicated data
     * @param recoverableDataTransformer transformer of the recoverable part of this partition
     */
    public DLCPartition(Q replicatedData,
        DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableDataTransformer) {
        this.replicatedData = replicatedData;
        this.recoverableDataTransformer = recoverableDataTransformer;
    }

    /** */
    public Q getReplicatedData() {
        return replicatedData;
    }

    /** */
    public DLCPartitionRecoverableTransformer<K, V, Q, W> getRecoverableDataTransformer() {
        return recoverableDataTransformer;
    }

    /** */
    public W getRecoverableData() {
        return recoverableData;
    }

    /** */
    public void setRecoverableData(W recoverableData) {
        this.recoverableData = recoverableData;
    }
}
