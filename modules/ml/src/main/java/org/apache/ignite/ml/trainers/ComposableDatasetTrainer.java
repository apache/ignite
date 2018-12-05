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

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Class that is used for type conversions in {@link DatasetTrainer} in fluent style.
 *
 * @param <I> Type of model input.
 * @param <O> Type of model output.
 * @param <M> Type of trained model.
 * @param <L> Type of labels.
 */
public class ComposableDatasetTrainer<I, O, M extends Model<I, O>, L> extends DatasetTrainer<M, L> {
    /** Dataset builder. */
    protected DatasetTrainer<M, L> delegate;

    /**
     * Wrap specified {@link DatasetTrainer} into {@link ComposableDatasetTrainer}.
     *
     * @param trainer Trainer to wrap.
     * @param <I> Type of input of model produced by trainer.
     * @param <O> Type of output of model produced by trainer.
     * @param <M> Type of model produced by trainer.
     * @param <L> Type of labels on which model is trained.
     * @return Instance of this class wrapping specified {@link DatasetTrainer}.
     */
    public static <I, O, M extends Model<I, O>, L> ComposableDatasetTrainer<I, O, M, L> of(
        DatasetTrainer<M, L> trainer) {
        return new ComposableDatasetTrainer<>(trainer);
    }

    /**
     * Create instance of this class wrapping specified {@link DatasetTrainer}.
     *
     * @param trainer Trainer to wrap.
     */
    private ComposableDatasetTrainer(DatasetTrainer<M, L> trainer) {
        this.delegate = trainer;
    }

    /** {@inheritDoc} */
    @Override public <K, V> M fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor) {
        return delegate.fit(datasetBuilder, featureExtractor, lbExtractor);
    }

    /** {@inheritDoc} */
    @Override public boolean checkState(M mdl) {
        return delegate.checkState(mdl);
    }

    /** {@inheritDoc} */
    @Override public <K, V> M updateModel(M mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return delegate.updateModel(mdl, datasetBuilder, featureExtractor, lbExtractor);
    }

    /**
     * Let this trainer produce model {@code mdl}. This method produces a trainer which produces {@code mdl1}, where
     * {@code mdl1 = mdl . f}, where dot symbol is understood in sense of functions composition.
     *
     * @param f Function inserted before produced model.
     * @param <I1> Type of produced model input.
     * @return New {@link DatasetTrainer} which produces composition of specified function and model produced by
     * original trainer.
     */
    public <I1> ComposableDatasetTrainer<I1, O, ModelAfterFunction<I, I1, O, M>, L> beforeTrainedModel(
        IgniteFunction<I1, I> f) {
        DatasetTrainer<M, L> self = this;
        return new ComposableDatasetTrainer<>(new DatasetTrainer<ModelAfterFunction<I, I1, O, M>, L>() {
            /** {@inheritDoc} */
            @Override public <K, V> ModelAfterFunction<I, I1, O, M> fit(DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return new ModelAfterFunction<>(self.fit(datasetBuilder, featureExtractor, lbExtractor), f);
            }

            /** {@inheritDoc} */
            @Override public boolean checkState(ModelAfterFunction<I, I1, O, M> mdl) {
                return self.checkState(mdl.model());
            }

            /** {@inheritDoc} */
            @Override public <K, V> ModelAfterFunction<I, I1, O, M> updateModel(
                ModelAfterFunction<I, I1, O, M> mdl,
                DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
                IgniteBiFunction<K, V, L> lbExtractor) {
                return new ModelAfterFunction<>(self.updateModel(mdl.model(), datasetBuilder, featureExtractor, lbExtractor), f);
            }
        });
    }

    /**
     * Let this trainer produce model {@code mdl}. This method produces a trainer which produces {@code mdl1}, where
     * {@code mdl1 = f . mdl}, where dot symbol is understood in sense of functions composition.
     *
     * @param f Function inserted before produced model.
     * @param <O1> Type of produced model output.
     * @return New {@link DatasetTrainer} which produces composition of specified function and model produced by
     * original trainer.
     */
    public <O1> ComposableDatasetTrainer<I, O1, ModelBeforeFunction<I, O, O1, M>, L> afterTrainedModel(
        IgniteFunction<O, O1> f) {
        DatasetTrainer<M, L> self = this;
        return new ComposableDatasetTrainer<>(new DatasetTrainer<ModelBeforeFunction<I, O, O1, M>, L>() {
            /** {@inheritDoc} */
            @Override public <K, V> ModelBeforeFunction<I, O, O1, M> fit(DatasetBuilder<K, V> datasetBuilder,
                IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
                return new ModelBeforeFunction<>(self.fit(datasetBuilder, featureExtractor, lbExtractor), f);
            }

            /** {@inheritDoc} */
            @Override public boolean checkState(ModelBeforeFunction<I, O, O1, M> mdl) {
                return self.checkState(mdl.model());
            }

            /** {@inheritDoc} */
            @Override public <K, V> ModelBeforeFunction<I, O, O1, M> updateModel(
                ModelBeforeFunction<I, O, O1, M> mdl,
                DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, Vector> featureExtractor,
                IgniteBiFunction<K, V, L> lbExtractor) {
                return new ModelBeforeFunction<>(self.updateModel(mdl.model(), datasetBuilder, featureExtractor, lbExtractor), f);
            }
        });
    }

    /**
     * Method for usage convenience with IDE's 'introduce variable' suggestions to avoid
     * introducing variables with complex generic constructions. Returns this object as
     * {@link DatasetTrainer} with partially erased information about model.
     *
     * @return This object.
     */
    public DatasetTrainer<? extends Model<I, O>, L> simplyTyped() {
        return this;
    }

    /** {@inheritDoc} */
    @Override public ComposableDatasetTrainer<I, O, M, L> withEnvironmentBuilder(
        LearningEnvironmentBuilder envBuilder) {
        delegate = delegate.withEnvironmentBuilder(envBuilder);

        return this;
    }

    /** {@inheritDoc} */
    @Override public <L1> ComposableDatasetTrainer<I, O, M, L1> withConvertedLabels(IgniteFunction<L1, L> new2Old) {
        return new ComposableDatasetTrainer<>(delegate.withConvertedLabels(new2Old));
    }

    /**
     * Model which is a composition of form {@code mdl . f}, where {@code mdl} is some model (called "original model"),
     * {@code f} is some function.
     *
     * @param <I> Type of input of original model.
     * @param <I1> Type of input of function applied before original model.
     * @param <O> Type of output of original model.
     * @param <M> Type of original model.
     */
    public static class ModelAfterFunction<I, I1, O, M extends Model<I, O>> implements Model<I1, O> {
        /** Original model. */
        private M mdl;

        /** Function applied before original model. */
        private IgniteFunction<I1, I> before;

        /**
         * Constructs instance of this class using specified model and function.
         *
         * @param mdl Model.
         * @param before Function applied before model.
         */
        public ModelAfterFunction(M mdl, IgniteFunction<I1, I> before) {
            this.mdl = mdl;
            this.before = before;
        }

        /** {@inheritDoc} */
        @Override public O apply(I1 i1) {
            return mdl.apply(before.apply(i1));
        }

        /**
         * Gets original model.
         *
         * @return Model.
         */
        public M model() {
            return mdl;
        }
    }

    /**
     * Model which is a composition of form {@code f . mdl}, where {@code mdl} is some model (called "original model"),
     * {@code f} is some function.
     *
     * @param <I> Type of input of original model.
     * @param <O> Type of output of original model.
     * @param <O1> Type of output of function.
     * @param <M> Type of original model.
     */
    public static class ModelBeforeFunction<I, O, O1, M extends Model<I, O>> implements Model<I, O1> {
        /** Original model. */
        private M mdl;

        /** Function applied after original model. */
        private IgniteFunction<O, O1> after;

        /**
         * Constructs instance of this class using specified model and function.
         *
         * @param mdl Model.
         * @param after Function applied after model.
         */
        public ModelBeforeFunction(M mdl, IgniteFunction<O, O1> after) {
            this.mdl = mdl;
            this.after = after;
        }

        /** {@inheritDoc} */
        @Override public O1 apply(I i1) {
            return after.apply(mdl.apply(i1));
        }

        /**
         * Gets model.
         *
         * @return Model.
         */
        public M model() {
            return mdl;
        }
    }
}
