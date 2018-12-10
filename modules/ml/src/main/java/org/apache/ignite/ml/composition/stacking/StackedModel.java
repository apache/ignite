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
import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Model consisting of two layers:
 * <pre>
 *     1. Submodels layer {@code (IS -> IA)}.
 *     2. Aggregator layer {@code (IA -> O)}.
 * </pre>
 * Submodels layer is a "parallel" composition of several models {@code IS -> IA} each of them getting same input
 * {@code IS} and produce own output, these outputs outputs {@code [IA]}
 * are combined into a single output with a given binary "merger" operator {@code IA -> IA -> IA}. Result of merge
 * is then passed to the aggregator layer.
 * Aggregator layer consists of a model {@code IA -> O}.
 *
 * @param <IS> Type of submodels input.
 * @param <IA> Type of submodels output (same as aggregator model input).
 * @param <O> Type of aggregator model output.
 * @param <AM> Type of aggregator model.
 */
public class StackedModel<IS, IA, O, AM extends Model<IA, O>> implements Model<IS, O> {
    /** Submodels layer. */
    private Model<IS, IA> subModelsLayer;

    /** Aggregator model. */
    private final AM aggregatorMdl;

    /** Models constituting submodels layer. */
    private List<Model<IS, IA>> submodels;

    /** Binary operator merging submodels outputs. */
    private final IgniteBinaryOperator<IA> aggregatingInputMerger;

    /**
     * Constructs instance of this class.
     *
     * @param aggregatorMdl Aggregator model.
     * @param aggregatingInputMerger Binary operator used to merge submodels outputs.
     * @param subMdlInput2AggregatingInput Function converting submodels input to aggregator input. (This function
     * is needed when in {@link StackedDatasetTrainer} option to keep original features is chosen).
     */
    StackedModel(AM aggregatorMdl,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        IgniteFunction<IS, IA> subMdlInput2AggregatingInput) {
        this.aggregatorMdl = aggregatorMdl;
        this.aggregatingInputMerger = aggregatingInputMerger;
        this.subModelsLayer = subMdlInput2AggregatingInput != null ? subMdlInput2AggregatingInput::apply : null;
        submodels = new ArrayList<>();
    }

    /**
     * Get submodels constituting first layer of this model.
     *
     * @return Submodels constituting first layer of this model.
     */
    List<Model<IS, IA>> submodels() {
        return submodels;
    }

    /**
     * Get aggregator model.
     *
     * @return Aggregator model.
     */
    AM aggregatorModel() {
        return aggregatorMdl;
    }

    /**
     * Add submodel into first layer.
     *
     * @param subMdl Submodel to add.
     */
    void addSubmodel(Model<IS, IA> subMdl) {
        submodels.add(subMdl);
        subModelsLayer = subModelsLayer != null ? subModelsLayer.combine(subMdl, aggregatingInputMerger)
            : subMdl;
    }

    /** {@inheritDoc} */
    @Override public O apply(IS is) {
        return subModelsLayer.andThen(aggregatorMdl).apply(is);
    }
}
