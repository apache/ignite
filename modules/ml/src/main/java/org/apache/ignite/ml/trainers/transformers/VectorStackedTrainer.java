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

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class VectorStackedTrainer<O, L, AM extends Model<Vector, O>> extends StackedDatasetTrainer<Vector, Vector, O, AM, L> {
    public VectorStackedTrainer(DatasetTrainer<AM, L> aggregatingTrainer) {
        super(aggregatingTrainer, VectorUtils::concat, IgniteFunction.identity());
    }

    public <O1, M extends Model<Vector, O1>> StackedDatasetTrainer<Vector, Vector, O, AM, L> withAddedTrainer(
        DatasetTrainer<M, L> trainer,
        IgniteFunction<O1, Vector> output2VecConverter,
        IgniteFunction<O1, Vector> submodelOutput2AggregatingInputConverter) {
        return super.withAddedTrainer(trainer, IgniteFunction.identity(), output2VecConverter, submodelOutput2AggregatingInputConverter);
    }
}
