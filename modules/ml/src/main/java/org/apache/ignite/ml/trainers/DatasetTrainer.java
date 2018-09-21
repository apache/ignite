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

package org.apache.ignite.ml.trainers;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for trainers. Trainer is just a function which produces model from the data.
 *
 * @param <M> Type of a produced model.
 * @param <L> Type of a label.
 */
public abstract class DatasetTrainer<M extends Model, L> {
    /** Learning Environment. */
    protected LearningEnvironment environment = LearningEnvironment.DEFAULT;

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public abstract <K, V> M fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor);

    /**
     * Gets state of model in arguments, compare it with training parameters of trainer and if they are fit then
     * trainer updates model in according to new data and return new model. In other case trains new model.
     *
     * @param mdl Learned model.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K,V> M update(M mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {

        if(mdl != null) {
            if(checkState(mdl)) {
                return updateModel(mdl, datasetBuilder, featureExtractor, lbExtractor);
            } else {
                environment.logger(getClass()).log(
                    MLLogger.VerboseLevel.HIGH,
                    "Model cannot be updated because of initial state of " +
                        "it doesn't corresponds to trainer parameters"
                );
            }
        }

        return fit(datasetBuilder, featureExtractor, lbExtractor);
    }

    /**
     * @param mdl Model.
     * @return true if current critical for training parameters correspond to parameters from last training.
     */
    protected abstract boolean checkState(M mdl);

    /**
     * Used on update phase when given dataset is empty.
     * If last trained model exist then method returns it. In other case throws IllegalArgumentException.
     *
     * @param lastTrainedMdl Model.
     */
    @NotNull protected M getLastTrainedModelOrThrowEmptyDatasetException(M lastTrainedMdl) {
        String msg = "Cannot train model on empty dataset";
        if (lastTrainedMdl != null) {
            environment.logger(getClass()).log(MLLogger.VerboseLevel.HIGH, msg);
            return lastTrainedMdl;
        } else
            throw new EmptyDatasetException();
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    protected abstract <K, V> M updateModel(M mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor);

    /**
     * Trains model based on the specified data.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Ignite ignite, IgniteCache<K, V> cache,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new CacheBasedDatasetBuilder<>(ignite, cache),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V> M update(M mdl, Ignite ignite, IgniteCache<K, V> cache,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return update(
            mdl, new CacheBasedDatasetBuilder<>(ignite, cache),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param filter Filter for {@code upstream} data.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Ignite ignite, IgniteCache<K, V> cache, IgniteBiPredicate<K, V> filter,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new CacheBasedDatasetBuilder<>(ignite, cache, filter),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param filter Filter for {@code upstream} data.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V> M update(M mdl, Ignite ignite, IgniteCache<K, V> cache, IgniteBiPredicate<K, V> filter,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return update(
            mdl, new CacheBasedDatasetBuilder<>(ignite, cache, filter),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param data Data.
     * @param parts Number of partitions.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Map<K, V> data, int parts, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new LocalDatasetBuilder<>(data, parts),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param data Data.
     * @param parts Number of partitions.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V> M update(M mdl, Map<K, V> data, int parts, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return update(
            mdl, new LocalDatasetBuilder<>(data, parts),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param data Data.
     * @param filter Filter for {@code upstream} data.
     * @param parts Number of partitions.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V> M fit(Map<K, V> data, IgniteBiPredicate<K, V> filter, int parts,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(
            new LocalDatasetBuilder<>(data, filter, parts),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param data Data.
     * @param filter Filter for {@code upstream} data.
     * @param parts Number of partitions.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V> M update(M mdl, Map<K, V> data, IgniteBiPredicate<K, V> filter, int parts,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return update(
            mdl, new LocalDatasetBuilder<>(data, filter, parts),
            featureExtractor,
            lbExtractor
        );
    }

    /**
     * Sets learning Environment
     * @param environment Environment.
     */
    public void setEnvironment(LearningEnvironment environment) {
        this.environment = environment;
    }

    /**
     * EmptyDataset exception.
     */
    public static class EmptyDatasetException extends IllegalArgumentException {
        /** Serial version uid. */
        private static final long serialVersionUID = 6914650522523293521L;

        /**
         * Constructs an instance of EmptyDatasetException.
         */
        public EmptyDatasetException() {
            super("Cannot train model on empty dataset");
        }
    }
}
