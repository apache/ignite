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
import javax.cache.Cache;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

/**
 * Transformer which transforms upstream data to any desired type of recoverable data.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 * @param <Q> type of replicated data of a partition
 * @param <W> type of recoverable data of a partition
 */
@FunctionalInterface
public interface DLCPartitionRecoverableDataTransformer<K, V, Q extends Serializable, W extends AutoCloseable>
    extends IgniteTriFunction<Iterable<Cache.Entry<K, V>>, Long, Q, W> {
    /** {@inheritDoc} */
    @Override public W apply(Iterable<Cache.Entry<K, V>> upstreamData, Long upstreamDataSize, Q replicatedData);
}
