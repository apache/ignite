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
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.composition.combinators.sequential.SameTrainersSequentialComposition;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class SameTrainersParallelComposition<I, O, L>
    extends DatasetTrainer<Model<I, List<O>>, L> {
    private final List<DatasetTrainer<Model<I, O>, L>> trainers;

    public SameTrainersParallelComposition(List<DatasetTrainer<Model<I, O>, L>> trainers) {
        this.trainers = new ArrayList<>(trainers);
    }

    public static <I, O, M extends Model<I, O>, T extends DatasetTrainer<M, L>, L> SameTrainersParallelComposition<I, O, L> of(List<T> trainers) {
        List<DatasetTrainer<Model<I, O>, L>> trs =
            trainers.stream().map(CompositionUtils::unsafeCoerce).collect(Collectors.toList());

        return new SameTrainersParallelComposition<>(trs);
    }

    @Override public <K, V> Model<I, List<O>> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        List<IgniteSupplier<Model<I, O>>> tasks = trainers.stream()
            .map(tr -> (IgniteSupplier<Model<I, O>>)(() -> tr.fit(datasetBuilder, featureExtractor, lbExtractor)))
            .collect(Collectors.toList());

        List<Model<I, O>> mdls = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        return new SameModelsParallelComposition<>(mdls);
    }

    @Override public <K, V> Model<I, List<O>> update(Model<I, List<O>> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        // Unsafe.
        SameModelsParallelComposition<I, O> typedMdl = (SameModelsParallelComposition<I, O>)mdl;

        assert typedMdl.models().size() == trainers.size();
        List<Model<I, O>> mdls = new ArrayList<>();

        for (int i = 0; i < trainers.size(); i++)
            mdls.add(trainers.get(i).update(typedMdl.models().get(i), datasetBuilder, featureExtractor, lbExtractor));

        return new SameModelsParallelComposition<>(mdls);
    }

    @Override protected boolean checkState(Model<I, List<O>> mdl) {
        // Never called.
        return false;
    }

    @Override protected <K, V> Model<I, List<O>> updateModel(Model<I, List<O>> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        // Never called.
        return null;
    }
}
