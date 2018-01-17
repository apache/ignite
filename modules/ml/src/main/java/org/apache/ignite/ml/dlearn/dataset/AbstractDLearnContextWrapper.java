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

package org.apache.ignite.ml.dlearn.dataset;

import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;

/**
 * Wrapper of a learning context which allows to introduce new context-specific methods based on base {@code compute()}
 * functionality.
 *
 * @param <P> type of a d-learn partition
 */
public class AbstractDLearnContextWrapper<P extends AutoCloseable> implements DLearnContext<P> {
    /**
     * Delegate which actually performs base functions like {@code compute()}, {@code transform()} and {@code close()}.
     */
    protected final DLearnContext<P> delegate;

    /**
     * Constructs a new instance of context wrapper which delegates base operations to {@code delegate}.
     *
     * @param delegate delegate
     */
    public AbstractDLearnContextWrapper(DLearnContext<P> delegate) {
        this.delegate = delegate;
    }

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<P, Integer, R> mapper, IgniteBinaryOperator<R> reducer) {
        return delegate.compute(mapper, reducer);
    }

    /** {@inheritDoc} */
    @Override public void compute(IgniteBiConsumer<P, Integer> mapper) {
        delegate.compute(mapper);
    }

    /** {@inheritDoc} */
    @Override public <T extends AutoCloseable, C extends DLearnContext<T>> C transform(
        DLearnContextTransformer<P, T, C> transformer) {
        return delegate.transform(transformer);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        delegate.close();
    }
}
