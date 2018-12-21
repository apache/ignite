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

package org.apache.ignite.ml.composition.combinators.parallel;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.combinators.sequential.SameTrainersSequentialComposition;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class SameTrainersParallelComposition<I, O, M extends Model<I, O>, L, T extends DatasetTrainer<M, L>>
    extends DatasetTrainer<SameModelsParallelComposition<I, O, M>, L> {
    private final List<T> trainers;

    public SameTrainersParallelComposition(T trainer, int n) {
        trainers = Collections.nCopies(n, trainer);
    }

    public SameTrainersParallelComposition(List<T> trainers) {
        this.trainers = trainers;
    }

    @Override public <K, V> SameModelsParallelComposition<I, O, M> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        List<IgniteSupplier<M>> tasks = trainers.stream()
            .map(tr -> (IgniteSupplier<M>)(() -> tr.fit(datasetBuilder, featureExtractor, lbExtractor)))
            .collect(Collectors.toList());

        List<M> mdls = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        return new SameModelsParallelComposition<>(mdls);
    }

    @Override public <K, V> SameModelsParallelComposition<I, O, M> update(SameModelsParallelComposition<I, O, M> mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        assert mdl.models().size() == trainers.size();

        List<M> updatedModels = new ArrayList<>();

        for (int i = 0; i < mdl.models().size(); i++)
            updatedModels.add(trainers.get(i).update(mdl.models().get(i), datasetBuilder, featureExtractor, lbExtractor));

        return new SameModelsParallelComposition<>(updatedModels);
    }

    @Override protected boolean checkState(SameModelsParallelComposition<I, O, M> mdl) {
        // Never called.
        return false;
    }

    @Override protected <K, V> SameModelsParallelComposition<I, O, M> updateModel(SameModelsParallelComposition<I, O, M> mdl,
        DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        // Never called.
        return null;
    }
}
