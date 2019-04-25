/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.composition;

import java.io.Serializable;
import java.util.List;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * ModelsComposition representation.
 *
 * @see ModelsComposition
 */
public class ModelsCompositionFormat implements Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 9115341364082681837L;

    /** Models. */
    private List<IgniteModel<Vector, Double>> models;

    /** Predictions aggregator. */
    private PredictionsAggregator predictionsAggregator;

    /**
     * Creates an instance of ModelsCompositionFormat.
     *
     * @param models Models.
     * @param predictionsAggregator Predictions aggregator.
     */
    public ModelsCompositionFormat(List<IgniteModel<Vector, Double>> models,PredictionsAggregator predictionsAggregator) {
        this.models = models;
        this.predictionsAggregator = predictionsAggregator;
    }

    /** */
    public List<IgniteModel<Vector, Double>> models() {
        return models;
    }

    /** */
    public PredictionsAggregator predictionsAggregator() {
        return predictionsAggregator;
    }
}
