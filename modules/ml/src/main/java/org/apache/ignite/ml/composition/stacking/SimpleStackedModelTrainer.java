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

import java.util.ArrayList;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.stacking.StackedDatasetTrainer;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * {@link DatasetTrainer} with same type of input and output of submodels.
 *
 * @param <I> Type of submodels input.
 * @param <O> Type of aggregator model output.
 * @param <AM> Type of aggregator model.
 * @param <L> Type of labels.
 */
public class SimpleStackedModelTrainer<I, O, AM extends Model<I, O>, L> extends StackedDatasetTrainer<I, I, O, AM, L> {
    /**
     * Construct instance of this class.
     *SS
     * @param aggregatingTrainer Aggregator trainer.
     * @param aggregatingInputMerger Function used to merge submodels outputs into one.
     * @param submodelInput2AggregatingInputConverter Function used to convert input of submodel to output of submodel
     * this function is used if user chooses to keep original features.
     */
    public SimpleStackedModelTrainer(DatasetTrainer<AM, L> aggregatingTrainer,
        IgniteBinaryOperator<I> aggregatingInputMerger,
        IgniteFunction<I, I> submodelInput2AggregatingInputConverter,
        IgniteFunction<Vector, I> vector2SubmodelInputConverter,
        IgniteFunction<I, Vector> submodelOutput2VectorConverter) {
        super(aggregatingTrainer,
            aggregatingInputMerger,
            submodelInput2AggregatingInputConverter,
            new ArrayList<>(),
            vector2SubmodelInputConverter,
            submodelOutput2VectorConverter);
    }

    /**
     * Constructs instance of this class.
     */
    public SimpleStackedModelTrainer() {
        super();
    }

    /**
     * Construct instance of this class.
     *
     * @param aggregatingTrainer Aggregator trainer.
     * @param aggregatingInputMerger Function used to merge submodels outputs into one.
     */
    public SimpleStackedModelTrainer(DatasetTrainer<AM, L> aggregatingTrainer,
        IgniteBinaryOperator<I> aggregatingInputMerger) {
        super(aggregatingTrainer, aggregatingInputMerger, IgniteFunction.identity());
    }

    /**
     * Keep original features using {@link IgniteFunction#identity()} as submodelInput2AggregatingInputConverter.
     *
     * @return This object.
     */
    public StackedDatasetTrainer<I, I, O, AM, L> withOriginalFeaturesKept() {
        return super.withOriginalFeaturesKept(IgniteFunction.identity());
    }
}
