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
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * This class represents a parallel composition of trainers.
 * Parallel composition of trainers is a trainer itself which trains a list of trainers with same
 * input and output. Training is done in following manner:
 * <pre>
 *     1. Independently train all trainers on the same dataset and get a list of models.
 *     2. Combine models produced in step (1) into a {@link ModelsParallelComposition}.
 * </pre>
 * Updating is made in a similar fashion.
 * Like in other trainers combinators we avoid to include type of contained trainers in type parameters
 * because otherwise compositions of compositions would have a relatively complex generic type which will
 * reduce readability.
 *
 * @param <I> Type of trainers inputs.
 * @param <O> Type of trainers outputs.
 * @param <L> Type of dataset labels.
 */
public class TrainersParallelComposition<I, O, L> extends DatasetTrainer<Model<I, List<O>>, L> {
    /** List of trainers. */
    private final List<DatasetTrainer<Model<I, O>, L>> trainers;

    /**
     * Construct an instance of this class from a list of trainers.
     *
     * @param trainers Trainers.
     * @param <M> Type of mode
     * @param <T>
     */
    public <M extends Model<I, O>, T extends DatasetTrainer<? extends Model<I, O>, L>> TrainersParallelComposition(
        List<T> trainers) {
        this.trainers = trainers.stream().map(CompositionUtils::unsafeCoerce).collect(Collectors.toList());
    }

    public static <I, O, M extends Model<I, O>, T extends DatasetTrainer<M, L>, L> TrainersParallelComposition<I, O, L> of(List<T> trainers) {
        List<DatasetTrainer<Model<I, O>, L>> trs =
            trainers.stream().map(CompositionUtils::unsafeCoerce).collect(Collectors.toList());

        return new TrainersParallelComposition<>(trs);
    }

    /** {@inheritDoc} */
    @Override public <K, V> Model<I, List<O>> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        List<IgniteSupplier<Model<I, O>>> tasks = trainers.stream()
            .map(tr -> (IgniteSupplier<Model<I, O>>)(() -> tr.fit(datasetBuilder, featureExtractor, lbExtractor)))
            .collect(Collectors.toList());

        List<Model<I, O>> mdls = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        return new ModelsParallelComposition<>(mdls);
    }

    /** {@inheritDoc} */
    @Override public <K, V> Model<I, List<O>> update(Model<I, List<O>> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        // Unsafe.
        ModelsParallelComposition<I, O> typedMdl = (ModelsParallelComposition<I, O>)mdl;

        assert typedMdl.models().size() == trainers.size();
        List<Model<I, O>> mdls = new ArrayList<>();

        for (int i = 0; i < trainers.size(); i++)
            mdls.add(trainers.get(i).update(typedMdl.models().get(i), datasetBuilder, featureExtractor, lbExtractor));

        return new ModelsParallelComposition<>(mdls);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(Model<I, List<O>> mdl) {
        // Never called.
        return false;
    }

    /** {@inheritDoc} */
    @Override protected <K, V> Model<I, List<O>> updateModel(Model<I, List<O>> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        // Never called.
        return null;
    }
}
