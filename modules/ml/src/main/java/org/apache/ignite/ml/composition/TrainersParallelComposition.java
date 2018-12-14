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

package org.apache.ignite.ml.composition;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class TrainersParallelComposition<I, O1, M1 extends Model<I, O1>, L, T1 extends DatasetTrainer<M1, L>,
    M2 extends Model<I, O1>, T2 extends DatasetTrainer<M2, L>, O2>
    extends DatasetTrainer<ModelsParallelComposition<I, O1, M1, M2, O2>, L> {
    private T1 tr1;
    private T2 tr2;
    private IgniteBiFunction<O1, O1, O2> merger;

    public TrainersParallelComposition(T1 tr1, T2 tr2, IgniteBiFunction<O1, O1, O2> merger) {
        this.tr1 = tr1;
        this.tr2 = tr2;
        this.merger = merger;
    }

    /** {@inheritDoc} */
    @Override public <K, V> ModelsParallelComposition<I, O1, M1, M2, O2> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        List<IgniteSupplier<Model<I, O1>>> ts = Arrays.asList(
            () -> tr1.fit(datasetBuilder, featureExtractor, lbExtractor),
            () -> tr2.fit(datasetBuilder, featureExtractor, lbExtractor));

        List<Model<I, O1>> mdls = environment.parallelismStrategy().submit(ts)
            .stream().map(Promise::unsafeGet).collect(Collectors.toList());

        return new ModelsParallelComposition<>((M1)mdls.get(0), (M2)mdls.get(1), merger);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(ModelsParallelComposition<I, O1, M1, M2, O2> mdl) {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> ModelsParallelComposition<I, O1, M1, M2, O2> updateModel(
        ModelsParallelComposition<I, O1, M1, M2, O2> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return null;
    }
}
