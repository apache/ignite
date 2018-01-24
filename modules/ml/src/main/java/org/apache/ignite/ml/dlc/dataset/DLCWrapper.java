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

package org.apache.ignite.ml.dlc.dataset;

import java.io.Serializable;
import org.apache.ignite.ml.dlc.DLC;
import org.apache.ignite.ml.dlc.DLCPartition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

/**
 * Wrapper of a Distributed Learning Context which allows to introduce new context-specific methods based on base
 * {@code compute()} functionality.
 *
 * @param <K> type of an upstream value key
 * @param <V> type of an upstream value
 * @param <Q> type of replicated data of a partition
 * @param <W> type of recoverable data of a partition
 */
public class DLCWrapper<K, V, Q extends Serializable, W extends AutoCloseable> implements DLC<K, V, Q, W> {
    /** Delegate which actually performs base functions like {@code compute()}  and {@code close()}. */
    private final DLC<K, V, Q, W> delegate;

    /**
     * Constructs a new instance of Distributed Learning Context wrapper
     *
     * @param delegate delegate which actually performs base functions
     */
    public DLCWrapper(DLC<K, V, Q, W> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<DLCPartition<K, V, Q, W>, Integer, R> mapper, IgniteBinaryOperator<R> reducer,
        R identity) {
        return delegate.compute(mapper, reducer, identity);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        delegate.close();
    }
}
