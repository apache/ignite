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
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class StackedModel<IS, IA, O, AM extends Model<IA, O>> implements Model<IS, O> {
    private Model<IS, IA> subModelsLayer;
    private final AM aggregatorModel;
    private List<Model<IS, IA>> submodels;
    private final IgniteBinaryOperator<IA> aggregatingInputMerger;

    StackedModel(AM aggregatorMdl,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        IgniteFunction<IS, IA> subMdlInput2AggregatingInput) {
        this.aggregatorModel = aggregatorMdl;
        this.aggregatingInputMerger = aggregatingInputMerger;
        this.subModelsLayer = subMdlInput2AggregatingInput != null ? subMdlInput2AggregatingInput::apply : null;
        submodels = new ArrayList<>();
    }

    List<Model<IS, IA>> submodels() {
        return submodels;
    }

    AM aggregatingModel() {
        return aggregatorModel;
    }

    void addSubmodel(Model<IS, IA> subModel) {
        submodels.add(subModel);
        subModelsLayer = subModelsLayer != null ? subModelsLayer.combine(subModelsLayer, aggregatingInputMerger)
            : subModel;
    }

    @Override public O apply(IS is) {
        return subModelsLayer.andThen(aggregatorModel).apply(is);
    }
}
