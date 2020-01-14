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

package org.apache.ignite.ml.composition.stacking;

import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.matrix.impl.DenseMatrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.AdaptableDatasetTrainer;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * {@link StackedDatasetTrainer} with {@link Vector} as submodels input and output.
 *
 * @param <O> Type of aggregator model output.
 * @param <L> Type of labels.
 * @param <AM> Type of aggregator model.
 */
public class StackedVectorDatasetTrainer<O, AM extends IgniteModel<Vector, O>, L>
    extends SimpleStackedDatasetTrainer<Vector, O, AM, L> {
    /**
     * Constructs instance of this class.
     *
     * @param aggregatingTrainer Aggregator trainer.
     */
    public StackedVectorDatasetTrainer(DatasetTrainer<AM, L> aggregatingTrainer) {
        super(aggregatingTrainer,
            VectorUtils::concat,
            IgniteFunction.identity(),
            IgniteFunction.identity(),
            IgniteFunction.identity());
    }

    /**
     * Constructs instance of this class.
     */
    public StackedVectorDatasetTrainer() {
        this(null);
    }

    /** {@inheritDoc} */
    @Override public <M1 extends IgniteModel<Vector, Vector>> StackedVectorDatasetTrainer<O, AM, L> addTrainer(
        DatasetTrainer<M1, L> trainer) {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.addTrainer(trainer);
    }

    //TODO: IGNITE-10441 -- Look for options to avoid boilerplate overrides.
    /** {@inheritDoc} */
    @Override public StackedVectorDatasetTrainer<O, AM, L> withAggregatorTrainer(
        DatasetTrainer<AM, L> aggregatorTrainer) {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withAggregatorTrainer(aggregatorTrainer);
    }

    /** {@inheritDoc} */
    @Override public StackedVectorDatasetTrainer<O, AM, L> withOriginalFeaturesKept() {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withOriginalFeaturesKept();
    }

    /** {@inheritDoc} */
    @Override public StackedVectorDatasetTrainer<O, AM, L> withOriginalFeaturesDropped() {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withOriginalFeaturesDropped();
    }

    /** {@inheritDoc} */
    // TODO: IGNITE-10843 Add possibility to keep features with specific indices.
    @Override public StackedVectorDatasetTrainer<O, AM, L> withOriginalFeaturesKept(
        IgniteFunction<Vector, Vector> submodelInput2AggregatingInputConverter) {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withOriginalFeaturesKept(
            submodelInput2AggregatingInputConverter);
    }

    /** {@inheritDoc} */
    @Override public StackedVectorDatasetTrainer<O, AM, L> withSubmodelOutput2VectorConverter(
        IgniteFunction<Vector, Vector> submodelOutput2VectorConverter) {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withSubmodelOutput2VectorConverter(
            submodelOutput2VectorConverter);
    }

    /** {@inheritDoc} */
    @Override public StackedVectorDatasetTrainer<O, AM, L> withVector2SubmodelInputConverter(
        IgniteFunction<Vector, Vector> vector2SubmodelInputConverter) {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withVector2SubmodelInputConverter(
            vector2SubmodelInputConverter);
    }

    /** {@inheritDoc} */
    @Override public StackedVectorDatasetTrainer<O, AM, L> withAggregatorInputMerger(
        IgniteBinaryOperator<Vector> merger) {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withAggregatorInputMerger(merger);
    }

    /** {@inheritDoc} */
    @Override public StackedVectorDatasetTrainer<O, AM, L> withEnvironmentBuilder(
        LearningEnvironmentBuilder envBuilder) {
        return (StackedVectorDatasetTrainer<O, AM, L>)super.withEnvironmentBuilder(envBuilder);
    }

    /** {@inheritDoc} */
    @Override public <L1> StackedVectorDatasetTrainer<O, AM, L1> withConvertedLabels(
        IgniteFunction<L1, L> new2Old) {
        return (StackedVectorDatasetTrainer<O, AM, L1>)super.withConvertedLabels(new2Old);
    }

    /**
     * Shortcut for adding trainer {@code Vector -> Double} where this trainer is treated as {@code Vector -> Vector}, where
     * output {@link Vector} is constructed by wrapping double value.
     *
     * @param trainer Submodel trainer.
     * @param <M1> Type of submodel trainer model.
     * @return This object.
     */
    public <M1 extends IgniteModel<Vector, Double>> StackedVectorDatasetTrainer<O, AM, L> addTrainerWithDoubleOutput(
        DatasetTrainer<M1, L> trainer) {
        return addTrainer(AdaptableDatasetTrainer.of(trainer).afterTrainedModel(VectorUtils::num2Vec));
    }

    /**
     * Shortcut for adding trainer {@code Matrix -> Matrix} where this trainer is treated as {@code Vector -> Vector}, where
     * input {@link Vector} is turned into {@code 1 x cols} {@link Matrix} and output is a first row of output {@link Matrix}.
     *
     * @param trainer Submodel trainer.
     * @param <M1> Type of submodel trainer model.
     * @return This object.
     */
    public <M1 extends IgniteModel<Matrix, Matrix>> StackedVectorDatasetTrainer<O, AM, L> addMatrix2MatrixTrainer(
        DatasetTrainer<M1, L> trainer) {
        AdaptableDatasetTrainer<Vector, Vector, Matrix, Matrix, M1, L> adapted = AdaptableDatasetTrainer.of(trainer)
            .beforeTrainedModel((Vector v) -> new DenseMatrix(v.asArray(), 1))
            .afterTrainedModel((Matrix mtx) -> mtx.getRow(0));

        return addTrainer(adapted);
    }
}
