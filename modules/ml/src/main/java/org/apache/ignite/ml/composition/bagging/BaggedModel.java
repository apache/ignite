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

package org.apache.ignite.ml.composition.bagging;

import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

// TODO: write about reason why it is not general.
public class BaggedModel implements IgniteModel<Vector, Double> {
    private IgniteModel<Vector, List<Double>> mdl;
    private PredictionsAggregator aggregator;

    BaggedModel(IgniteModel<Vector, List<Double>> mdl, PredictionsAggregator aggregator) {
        this.mdl = mdl;
        this.aggregator = aggregator;
    }

    IgniteModel<Vector, List<Double>> model() {
        return mdl;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector i) {
        return mdl.andThen((IgniteFunction<List<Double>, Double>)l ->
            aggregator.apply(l.stream().mapToDouble(Double::valueOf).toArray())).predict(i);
    }
}
