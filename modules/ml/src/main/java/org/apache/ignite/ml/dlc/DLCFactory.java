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
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * DLC factory which produces distributed learning contexts.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 */
public interface DLCFactory<K, V> {
    /**
     * Constructs a new instance of distributed learning context using specified replicated data transformer,
     * recoverable data transformer and DLC wrapper.
     *
     * @param replicatedTransformer replicated data transformer
     * @param recoverableTransformer recoverable data transformer
     * @param wrapDLC DLC wrapper
     * @param <Q> type of replicated data of a partition
     * @param <W> type of recoverable data of a partition
     * @param <I> type of returned learning context
     * @return distributed learning context
     */
    public <Q extends Serializable, W extends AutoCloseable, I extends DLC<K, V, Q, W>> I createDLC(
        DLCPartitionReplicatedTransformer<K, V, Q> replicatedTransformer,
        DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableTransformer,
        IgniteFunction<DLC<K, V, Q, W>, I> wrapDLC);

    /**
     * Constructs a new instance of distributed learning context using specified replicated data transformer and
     * replicated data transformer.
     *
     * @param replicatedTransformer replicated data transformer
     * @param recoverableTransformer recoverable data transformer
     * @param <Q> type of an upstream value
     * @param <W> type of recoverable data of a partition
     * @return  distributed learning context
     */
    default public <Q extends Serializable, W extends AutoCloseable> DLC<K, V, Q, W> cteateDLC(
        DLCPartitionReplicatedTransformer<K, V, Q> replicatedTransformer,
        DLCPartitionRecoverableTransformer<K, V, Q, W> recoverableTransformer) {
        return createDLC(replicatedTransformer, recoverableTransformer, e -> e);
    }
}
