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
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Type used to convert input and output types of wrapped {@link DatasetTrainer}.
 * Produces model which is combination  of form {@code after . wMdl . before} where dot denotes functional composition
 * and wMdl is model produced by wrapped trainer.
 *
 * @param <I> Input type of model produced by this trainer.
 * @param <O> Output type of model produced by this trainer.
 * @param <IW> Input type of model produced by wrapped trainer.
 * @param <OW> Output type of model produced by wrapped trainer.
 * @param <M> Type of model produced by wrapped model.
 * @param <L> Type of labels.
 */
public class CDT<I, O, IW, OW, M extends Model<IW, OW>, L> extends DatasetTrainer<CDM<I, O, IW, OW, M>, L> {
    /** Wrapped trainer. */
    private final DatasetTrainer<M, L> wrapped;

    /** Function used to convert input type of wrapped trainer. */
    private final IgniteFunction<I, IW> before;

    /** Function used to convert output type of wrapped trainer. */
    private final IgniteFunction<OW, O> after;

    /**
     * Construct instance of this class from a given {@link DatasetTrainer}.
     *
     * @param wrapped Wrapped trainer.
     * @param <I> Input type of wrapped trainer.
     * @param <O> Output type of wrapped trainer.
     * @param <M> Type of model produced by wrapped trainer.
     * @param <L> Type of labels.
     * @return Instance of this class.
     */
    public static <I, O, M extends Model<I, O>, L> CDT<I, O, I, O, M, L> of(DatasetTrainer<M, L> wrapped) {
        return new CDT<>(IgniteFunction.identity(), wrapped, IgniteFunction.identity());
    }

    /**
     * Construct instance of this class with specified wrapped trainer and converter functions.
     *
     * @param before Function used to convert input type of wrapped trainer.
     * @param wrapped  Wrapped trainer.
     * @param after Function used to convert output type of wrapped trainer.
     */
    private CDT(IgniteFunction<I, IW> before, DatasetTrainer<M, L> wrapped, IgniteFunction<OW, O> after) {
        this.before = before;
        this.wrapped = wrapped;
        this.after = after;
    }

    /** {@inheritDoc} */
    @Override public <K, V> CDM<I, O, IW, OW, M> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        M fit = wrapped.fit(datasetBuilder, featureExtractor, lbExtractor);
        return new CDM<>(before, fit, after);
    }

    /** {@inheritDoc} */
    @Override protected boolean checkState(CDM<I, O, IW, OW, M> mdl) {
        return wrapped.checkState(mdl.innerModel());
    }

    /** {@inheritDoc} */
    @Override protected <K, V> CDM<I, O, IW, OW, M> updateModel(CDM<I, O, IW, OW, M> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return mdl.withInnerModel(wrapped.updateModel(mdl.innerModel(), datasetBuilder, featureExtractor, lbExtractor));
    }

    /**
     * Let this trainer produce model {@code mdl}. This method produces a trainer which produces {@code mdl1}, where
     * {@code mdl1 = f . mdl}, where dot symbol is understood in sense of functions composition.
     *
     * @param after Function inserted before produced model.
     * @param <O1> Type of produced model output.
     * @return New {@link DatasetTrainer} which produces composition of specified function and model produced by
     * original trainer.
     */
    public <O1> CDT<I, O1, IW, OW, M, L> afterTrainedModel(IgniteFunction<O, O1> after) {
        return new CDT<>(before, wrapped, i -> after.apply(this.after.apply(i)));
    }

    /**
     * Let this trainer produce model {@code mdl}. This method produces a trainer which produces {@code mdl1}, where
     * {@code mdl1 = mdl . f}, where dot symbol is understood in sense of functions composition.
     *
     * @param before Function inserted before produced model.
     * @param <I1> Type of produced model input.
     * @return New {@link DatasetTrainer} which produces composition of specified function and model produced by
     * original trainer.
     */
    public <I1> CDT<I1, O, IW, OW, M, L> beforeTrainedModel(IgniteFunction<I1, I> before) {
        IgniteFunction<I1, IW> function = i -> this.before.apply(before.apply(i));
        return new CDT<>(function, wrapped, after);
    }
}
