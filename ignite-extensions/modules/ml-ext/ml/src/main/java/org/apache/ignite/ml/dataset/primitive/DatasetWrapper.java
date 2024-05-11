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

package org.apache.ignite.ml.dataset.primitive;

import java.io.Serializable;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

/**
 * A dataset wrapper that allows to introduce new functionality based on common {@code compute} methods.
 *
 * @param <C> Type of a partition {@code context}.
 * @param <D> Type of a partition {@code data}.
 *
 * @see SimpleDataset
 * @see SimpleLabeledDataset
 */
public class DatasetWrapper<C extends Serializable, D extends AutoCloseable> implements Dataset<C, D> {
    /** Delegate that performs {@code compute} actions. */
    protected final Dataset<C, D> delegate;

    /**
     * Constructs a new instance of dataset wrapper that delegates {@code compute} actions to the actual delegate.
     *
     * @param delegate Delegate that performs {@code compute} actions.
     */
    public DatasetWrapper(Dataset<C, D> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public <R> R computeWithCtx(IgniteTriFunction<C, D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce,
        R identity) {
        return delegate.computeWithCtx(map, reduce, identity);
    }

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        return delegate.compute(map, reduce, identity);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        delegate.close();
    }
}
