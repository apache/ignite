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

package org.apache.ignite.ml.dataset.api;

import java.io.Serializable;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

public class DatasetWrapper<C extends Serializable, D extends AutoCloseable> implements Dataset<C, D> {

    protected final Dataset<C, D> delegate;

    public DatasetWrapper(Dataset<C, D> delegate) {
        this.delegate = delegate;
    }

    @Override public <R> R computeWithCtx(IgniteTriFunction<C, D, Integer, R> map, IgniteBinaryOperator<R> reduce,
        R identity) {
        return delegate.computeWithCtx(map, reduce, identity);
    }

    @Override public <R> R compute(IgniteBiFunction<D, Integer, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        return delegate.compute(map, reduce, identity);
    }

    @Override public void close() throws Exception {
        delegate.close();
    }
}
