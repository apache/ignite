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

package org.apache.ignite.ml.trainers.transformers;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.parallelism.Promise;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class StackedDatasetTrainer<IA, AM extends Model<IA, ?>, L> extends DatasetTrainer<AM, L> {
    private final IgniteFunction<IA, IA> aggregatingTrainerInputAdapter;
    public List<DatasetTrainer<? extends Model<Vector, Vector>, L>> ensemble;
    public DatasetTrainer<AM, L> aggregatingTrainer;

    public StackedDatasetTrainer(DatasetTrainer<AM, L> aggregatingTrainer,
        IgniteFunction<IA, IA> aggregatingTrainerInputAdapter) {
        this.aggregatingTrainer = aggregatingTrainer;
        this.aggregatingTrainerInputAdapter = aggregatingTrainerInputAdapter;
        this.ensemble = new ArrayList<>();
    }

    public <I, O> StackedDatasetTrainer<IA, AM, L> withAddedTrainer(DatasetTrainer<Model<I, O>, L> trainer,
        IgniteFunction<Vector, I> vec2InputConverter,
        IgniteFunction<I, Vector> input2VecConverter,
        IgniteFunction<O, Vector> output2VecConverter,
        IgniteFunction<Vector, O> vector2OutputConverter) {
        DatasetTrainer<Model<Vector, Vector>, L> wrappedTrainer = new DatasetTrainer<Model<Vector, Vector>, L>() {
            @Override public <K, V> Model<Vector, Vector> fit(DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                Model<I, O> mdl = trainer.fit(datasetBuilder, featureExtractor, lbExtractor);
                return toWrapped(mdl);
            }

            @Override protected boolean checkState(Model<Vector, Vector> mdl) {
                return trainer.checkState(toOriginal(mdl));
            }

            @Override protected <K, V> Model<Vector, Vector> updateModel(Model<Vector, Vector> mdl,
                DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return toWrapped(trainer.updateModel(toOriginal(mdl), datasetBuilder, featureExtractor, lbExtractor));
            }

            private Model<I, O> toOriginal(Model<Vector, Vector> mdl) {
                Model<I, I> id = Model.identityModel();
                return id.andThen(input2VecConverter).andThen(mdl).andThen(vector2OutputConverter);
            }

            private Model<Vector, Vector> toWrapped(Model<I, O> mdl) {
                Model<Vector, Vector> id = Model.identityModel();
                return id.andThen(vec2InputConverter).andThen(mdl).andThen(output2VecConverter);
            }
        };
        ensemble.add(wrappedTrainer);

        return this;
    }

    @Override public <K, V> AM fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        List<IgniteSupplier<Model<Vector, Vector>>> modelSuppliers = ensemble.stream()
            .map(tc -> (IgniteSupplier<Model<Vector, Vector>>)(() -> tc.fit(datasetBuilder, featureExtractor, lbExtractor)))
            .collect(Collectors.toList());

        List<Model<Vector, Vector>> submodels = environment.parallelismStrategy().submit(modelSuppliers).stream()
            .map(Promise::unsafeGet)
            .collect(Collectors.toList());

        // Add new columns consisting in submodels output in features.
        IgniteBiFunction<K, V, Vector> augmentedExtractor = featureExtractor.andThen((Vector v) -> {
            Vector[] objects = submodels.stream().map(sm -> sm.apply(v)).toArray(Vector[]::new);
            return VectorUtils.concat(v, objects);
        });

        AM mdl = aggregatingTrainer.fit(datasetBuilder, augmentedExtractor, lbExtractor);
    }

    @Override protected boolean checkState(AM mdl) {
        return false;
    }

    @Override protected <K, V> AM updateModel(AM mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        // First update
        return null;
    }

    private static class TrainerWithConverter<I, O, L> {
        DatasetTrainer<? extends Model<I, O>, L> trainer;
        IgniteFunction<Vector, I> inputConverter;
        IgniteFunction<O, Vector> outputConverter;
    }
}
