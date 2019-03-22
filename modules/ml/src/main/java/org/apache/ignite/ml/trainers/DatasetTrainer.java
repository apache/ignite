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
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.FeatureLabelExtractorWrapper;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;

/**
 * Interface for trainers. Trainer is just a function which produces model from the data.
 *
 * @param <M> Type of a produced model.
 * @param <L> Type of a label.
 */
public abstract class DatasetTrainer<M extends IgniteModel, L> {
    /** Learning environment builder. */
    protected LearningEnvironmentBuilder envBuilder = LearningEnvironmentBuilder.defaultBuilder();

    /** Learning Environment. */
    protected LearningEnvironment environment = envBuilder.buildForTrainer();

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
    @Deprecated //TODO: IGNITE-11481 - delete this method after ticket.
    public <K, V> M fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return fit(datasetBuilder, FeatureLabelExtractorWrapper.wrap(featureExtractor, lbExtractor));
    }

    /**
     * Returns the trainer which returns identity model.
     *
     * @param <I> Type of model input.
     * @param <L> Type of labels in dataset.
     * @return Trainer which returns identity model.
     */
    public static <I, L> DatasetTrainer<IgniteModel<I, I>, L> identityTrainer() {
        return new DatasetTrainer<IgniteModel<I, I>, L>() {
            /** {@inheritDoc} */
            @Override public <K, V, C> IgniteModel<I, I> fit(DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L> extractor) {
                return x -> x;
            }

            /** {@inheritDoc} */
            @Override protected <K, V, C> IgniteModel<I, I> updateModel(IgniteModel<I, I> mdl,
                DatasetBuilder<K, V> datasetBuilder, Vectorizer<K, V, C, L> extractor) {
                return x -> x;
            }

            /** {@inheritDoc} */
            @Override public boolean isUpdateable(IgniteModel<I, I> mdl) {
                return true;
            }
        };
    }

    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder Dataset builder.
     * @param extractor Extractor of {@link UpstreamEntry} into {@link LabeledVector}.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public abstract <K, V, C> M fit(DatasetBuilder<K, V> datasetBuilder, Vectorizer<K, V, C, L> extractor);

    /**
     * Gets state of model in arguments, compare it with training parameters of trainer and if they are fit then trainer
     * updates model in according to new data and return new model. In other case trains new model.
     *
     * @param mdl Learned model.
     * @param datasetBuilder Dataset builder.
     * @param extractor Extractor of {@link UpstreamEntry} into {@link LabeledVector}.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V, C> M update(M mdl, DatasetBuilder<K, V> datasetBuilder, Vectorizer<K, V, C, L> extractor) {

        if (mdl != null) {
            if (isUpdateable(mdl))
                return updateModel(mdl, datasetBuilder, extractor);
            else {
                environment.logger(getClass()).log(
                    MLLogger.VerboseLevel.HIGH,
                    "Model cannot be updated because of initial state of " +
                        "it doesn't corresponds to trainer parameters"
                );
            }
        }

        return fit(datasetBuilder, extractor);
    }

    /**
     * @param mdl Model.
     * @return True if current critical for training parameters correspond to parameters from last training.
     */
    public abstract boolean isUpdateable(M mdl);

    /**
     * Used on update phase when given dataset is empty. If last trained model exist then method returns it. In other
     * case throws IllegalArgumentException.
     *
     * @param lastTrainedMdl Model.
     */
    @NotNull protected M getLastTrainedModelOrThrowEmptyDatasetException(M lastTrainedMdl) {
        String msg = "Cannot train model on empty dataset";
        if (lastTrainedMdl != null) {
            environment.logger(getClass()).log(MLLogger.VerboseLevel.HIGH, msg);
            return lastTrainedMdl;
        }
        else
            throw new EmptyDatasetException();
    }

    /**
     * Trains model based on the specified data.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V, C> M fit(Ignite ignite, IgniteCache<K, V> cache, Vectorizer<K, V, C, L> vectorizer) {
        return fit(new CacheBasedDatasetBuilder<>(ignite, cache), vectorizer);
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V, C> M update(M mdl, Ignite ignite, IgniteCache<K, V> cache, Vectorizer<K, V, C, L> vectorizer) {
        return update(mdl, new CacheBasedDatasetBuilder<>(ignite, cache), vectorizer);
    }

    /**
     * Trains model based on the specified data.
     *
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param filter Filter for {@code upstream} data.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V, C> M fit(Ignite ignite, IgniteCache<K, V> cache, IgniteBiPredicate<K, V> filter,
        Vectorizer<K, V, C, L> vectorizer) {

        return fit(new CacheBasedDatasetBuilder<>(ignite, cache, filter), vectorizer);
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param ignite Ignite instance.
     * @param cache Ignite cache.
     * @param filter Filter for {@code upstream} data.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V, C> M update(M mdl, Ignite ignite, IgniteCache<K, V> cache, IgniteBiPredicate<K, V> filter,
        Vectorizer<K,V,C,L> vectorizer) {
        return update(
            mdl, new CacheBasedDatasetBuilder<>(ignite, cache, filter),
            vectorizer
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param data Data.
     * @param parts Number of partitions.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V, C> M fit(Map<K, V> data, int parts, Vectorizer<K, V, C, L> vectorizer) {
        return fit(new LocalDatasetBuilder<>(data, parts), vectorizer);
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param data Data.
     * @param parts Number of partitions.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V, C> M update(M mdl, Map<K, V> data, int parts, Vectorizer<K,V,C,L> vectorizer) {
        return update(
            mdl, new LocalDatasetBuilder<>(data, parts),
            vectorizer
        );
    }

    /**
     * Trains model based on the specified data.
     *
     * @param data Data.
     * @param filter Filter for {@code upstream} data.
     * @param parts Number of partitions.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Model.
     */
    public <K, V, C> M fit(Map<K, V> data, IgniteBiPredicate<K, V> filter, int parts,
        Vectorizer<K, V, C, L> vectorizer) {
        return fit(new LocalDatasetBuilder<>(data, filter, parts), vectorizer);
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param data Data.
     * @param filter Filter for {@code upstream} data.
     * @param parts Number of partitions.
     * @param vectorizer Upstream vectorizer.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    public <K, V, C> M update(M mdl, Map<K, V> data, IgniteBiPredicate<K, V> filter, int parts,
        Vectorizer<K,V,C,L> vectorizer) {
        return update(
            mdl, new LocalDatasetBuilder<>(data, filter, parts),
            vectorizer
        );
    }

    /**
     * Changes learning Environment.
     *
     * @param envBuilder Learning environment builder.
     */
    // TODO: IGNITE-10441 Think about more elegant ways to perform fluent API.
    public DatasetTrainer<M, L> withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        this.envBuilder = envBuilder;
        environment = envBuilder.buildForTrainer();

        return this;
    }

    /**
     * Gets state of model in arguments, update in according to new data and return new model.
     *
     * @param mdl Learned model.
     * @param datasetBuilder Dataset builder.
     * @param extractor Extractor of {@link UpstreamEntry} into {@link LabeledVector}.
     * @param <K> Type of a key in {@code upstream} data.
     * @param <V> Type of a value in {@code upstream} data.
     * @return Updated model.
     */
    protected abstract <K, V, C> M updateModel(M mdl,
        DatasetBuilder<K, V> datasetBuilder,
        Vectorizer<K, V, C, L> extractor);

    /**
     * Get learning environment.
     *
     * @return Learning environment.
     */
    public LearningEnvironment learningEnvironment() {
        return environment;
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

    /**
     * Creates {@link DatasetTrainer} with same training logic, but able to accept labels of given new type of labels.
     *
     * @param new2Old Converter of new labels to old labels.
     * @param <L1> New labels type.
     * @return {@link DatasetTrainer} with same training logic, but able to accept labels of given new type of labels.
     */
    public <L1> DatasetTrainer<M, L1> withConvertedLabels(IgniteFunction<L1, L> new2Old) {
        DatasetTrainer<M, L> old = this;
        return new DatasetTrainer<M, L1>() {
            private <K, V, C> Vectorizer<K, V, C, L> getNewExtractor(
                Vectorizer<K, V, C, L1> extractor) {
                return new Vectorizer.VectorizerAdapter<K, V, C, L>() {
                    /** {@inheritDoc} */
                    @Override public LabeledVector<L> apply(K k, V v) {
                        return new LabeledVector<>(extractor.extractFeatures(k, v),
                            new2Old.apply(extractor.extractLabel(k, v)));
                    }
                };
            }

            /** {@inheritDoc} */
            @Override public <K, V, C> M fit(DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L1> extractor) {
                return old.fit(datasetBuilder, getNewExtractor(extractor));
            }

            /** {@inheritDoc} */
            @Override protected <K, V, C> M updateModel(M mdl, DatasetBuilder<K, V> datasetBuilder,
                Vectorizer<K, V, C, L1> extractor) {
                return old.updateModel(mdl, datasetBuilder, getNewExtractor(extractor));
            }

            /** {@inheritDoc} */
            @Override public boolean isUpdateable(M mdl) {
                return old.isUpdateable(mdl);
            }
        };
    }
}
