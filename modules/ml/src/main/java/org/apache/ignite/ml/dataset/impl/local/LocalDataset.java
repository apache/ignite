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

package org.apache.ignite.ml.dataset.impl.local;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

/**
 * An implementation of dataset based on local data structures such as {@code Map} and {@code List} and doesn't require
 * Ignite environment. Introduces for testing purposes mostly, but can be used for simple local computations as well.
 *
 * @param <C> Type of a partition {@code context}.
 * @param <D> Type of a partition {@code data}.
 */
public class LocalDataset<C extends Serializable, D extends AutoCloseable> implements Dataset<C, D> {
    /** Partition {@code data} storage. */
    private final List<LearningEnvironment> envs;

    /** Partition {@code context} storage. */
    private final List<C> ctx;

    /** Partition {@code data} storage. */
    private final List<D> data;

    /**
     * Constructs a new instance of dataset based on local data structures such as {@code Map} and {@code List} and
     * doesn't requires Ignite environment.
     *
     * @param envs List of {@link LearningEnvironment}.
     * @param ctx Partition {@code context} storage.
     * @param data Partition {@code data} storage.
     */
    LocalDataset(List<LearningEnvironment> envs, List<C> ctx, List<D> data) {
        this.envs = envs;
        this.ctx = ctx;
        this.data = data;
    }

    /** {@inheritDoc} */
    @Override public <R> R computeWithCtx(IgniteTriFunction<C, D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce,
        R identity) {
        R res = identity;

        for (int part = 0; part < ctx.size(); part++) {
            D partData = data.get(part);
            LearningEnvironment env = envs.get(part);

            if (partData != null)
                res = reduce.apply(res, map.apply(ctx.get(part), partData, env));
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public <R> R compute(IgniteBiFunction<D, LearningEnvironment, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        R res = identity;

        for (int part = 0; part < data.size(); part++) {
            D partData = data.get(part);
            LearningEnvironment env = envs.get(part);

            if (partData != null)
                res = reduce.apply(res, map.apply(partData, env));
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        // Do nothing, GC will clean up.
    }

    /** */
    public List<C> getCtx() {
        return ctx;
    }

    /** */
    public List<D> getData() {
        return data;
    }
}
