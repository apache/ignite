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

import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.functions.IgniteFunction;

public class StackedModel<IS, IA, O, M extends Model<IA, O>> implements Model<IS, O> {
    private Model<IS, IA> subModelsLayer;
    private final M aggregatingModel;
    private List<Model<IS, ?>> submodels;
    private final IgniteBinaryOperator<IA> aggregatingInputMerger;

    public StackedModel(M aggregatingMdl,
        IgniteBinaryOperator<IA> aggregatingInputMerger,
        Model<IS, IA> subMdl,
        IgniteFunction<IS, IA> subModelInput2AggregatingInput) {
        this.aggregatingModel = aggregatingMdl;
        this.aggregatingInputMerger = aggregatingInputMerger;
        this.subModelsLayer = subMdl;
    }

//    public StackedModel(IgniteFunction<IS, IA> subModelInput2AggregatingInput) {
//
//    }
//
//    public StackedModel(Model<IS, IA> subMdl) {
//        subModelsLayer = subMdl;
//    }

    List<Model<IS, ?>> submodels() {
        return submodels;
    }

    M aggregatingModel() {
        return aggregatingModel;
    }

    StackedModel<IS, IA, O, M> withAddedSubmodel(Model<IS, IA> subModel) {
        submodels.add(subModel);
        subModelsLayer = subModelsLayer.combine(subModelsLayer, aggregatingInputMerger);

        return this;
    }

    @Override public O apply(IS is) {
        return subModelsLayer.andThen(aggregatingModel).apply(is);
    }
}
