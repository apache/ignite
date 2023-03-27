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
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * This class represents a parallel composition of trainers. Parallel composition of trainers is a trainer itself which
 * trains a list of trainers with same input and output. Training is done in following manner:
 * <pre>
 *     1. Independently train all trainers on the same dataset and get a list of models.
 *     2. Combine models produced in step (1) into a {@link ModelsParallelComposition}.
 * </pre>
 * Updating is made in a similar fashion. Like in other trainers combinators we avoid to include type of contained
 * trainers in type parameters because otherwise compositions of compositions would have a relatively complex generic
 * type which will reduce readability.
 *
 * @param <I> Type of trainers inputs.
 * @param <O> Type of trainers outputs.
 * @param <L> Type of dataset labels.
 */
public class TrainersParallelComposition<I, O, L> extends DatasetTrainer<IgniteModel<I, List<O>>, L> {
    /** List of trainers. */
    private final List<DatasetTrainer<IgniteModel<I, O>, L>> trainers;

    /**
     * Construct an instance of this class from a list of trainers.
     *
     * @param trainers Trainers.
     * @param <T> Type of trainer.
     */
    public <T extends DatasetTrainer<? extends IgniteModel<I, O>, L>> TrainersParallelComposition(
        List<T> trainers) {
        this.trainers = trainers.stream().map(CompositionUtils::unsafeCoerce).collect(Collectors.toList());
    }

    /**
     * Create parallel composition of trainers contained in a given list.
     *
     * @param trainers List of trainers.
     * @param <I> Type of input of model priduced by trainers.
     * @param <O> Type of output of model priduced by trainers.
     * @param <M> Type of model priduced by trainers.
     * @param <T> Type of trainers.
     * @param <L> Type of input of labels.
     * @return Parallel composition of trainers contained in a given list.
     */
    public static <I, O, M extends IgniteModel<I, O>, T extends DatasetTrainer<M, L>, L> TrainersParallelComposition<I, O, L> of(
        List<T> trainers) {
        List<DatasetTrainer<IgniteModel<I, O>, L>> trs =
            trainers.stream().map(CompositionUtils::unsafeCoerce).collect(Collectors.toList());

        return new TrainersParallelComposition<>(trs);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteModel<I, List<O>> fitWithInitializedDeployingContext(DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        List<IgniteSupplier<IgniteModel<I, O>>> tasks = trainers.stream()
            .map(tr -> (IgniteSupplier<IgniteModel<I, O>>)(() -> tr.fit(datasetBuilder, preprocessor)))
            .collect(Collectors.toList());

        List<IgniteModel<I, O>> mdls = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        return new ModelsParallelComposition<>(mdls);
    }

    /** {@inheritDoc} */
    @Override public <K, V> IgniteModel<I, List<O>> update(IgniteModel<I, List<O>> mdl,
        DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        learningEnvironment().initDeployingContext(preprocessor);

        ModelsParallelComposition<I, O> typedMdl = (ModelsParallelComposition<I, O>)mdl;

        assert typedMdl.submodels().size() == trainers.size();
        List<IgniteSupplier<IgniteModel<I, O>>> tasks = new ArrayList<>();

        for (int i = 0; i < trainers.size(); i++) {
            int j = i;
            tasks.add(() -> trainers.get(j).update(typedMdl.submodels().get(j), datasetBuilder, preprocessor));
        }

        List<IgniteModel<I, O>> mdls = environment.parallelismStrategy().submit(tasks).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        return new ModelsParallelComposition<>(mdls);
    }

    /**
     * This method is never called, instead of constructing logic of update from {@link DatasetTrainer#isUpdateable} and
     * {@link DatasetTrainer#updateModel} in this class we explicitly override update method.
     *
     * @param mdl Model.
     * @return True if current critical for training parameters correspond to parameters from last training.
     */
    @Override public boolean isUpdateable(IgniteModel<I, List<O>> mdl) {
        // Never called.
        throw new IllegalStateException();
    }

    /**
     * This method is never called, instead of constructing logic of update from {@link
     * DatasetTrainer#isUpdateable(IgniteModel)} and {@link DatasetTrainer#updateModel(IgniteModel, DatasetBuilder, Preprocessor)}
     * in this class we explicitly override update method.
     *
     * @param mdl Model.
     * @return Updated model.
     */
    @Override protected <K, V> IgniteModel<I, List<O>> updateModel(IgniteModel<I, List<O>> mdl,
        DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> preprocessor) {
        // Never called.
        throw new IllegalStateException();
    }
}
