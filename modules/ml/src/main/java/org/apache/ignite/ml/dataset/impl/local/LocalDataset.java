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
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteTriFunction;

public class LocalDataset<C extends Serializable, D extends AutoCloseable> implements Dataset<C, D> {

    private final List<C> ctx;

    private final List<D> data;

    public LocalDataset(List<C> ctx, List<D> data) {
        this.ctx = ctx;
        this.data = data;
    }

    @Override public <R> R computeWithCtx(IgniteTriFunction<C, D, Integer, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        R res = identity;

        for (int part = 0; part < ctx.size(); part++)
            res = reduce.apply(res, map.apply(ctx.get(part), data.get(part), part));

        return res;
    }

    @Override public <R> R compute(IgniteBiFunction<D, Integer, R> map, IgniteBinaryOperator<R> reduce, R identity) {
        R res = identity;

        for (int part = 0; part < data.size(); part++)
            res = reduce.apply(res, map.apply(data.get(part), part));

        return res;
    }

    @Override public void close() {
        // Do nothing.
    }
}
