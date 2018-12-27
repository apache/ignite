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

package org.apache.ignite.ml.xgboost;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.ignite.ml.tree.DecisionTreeNode;

import static org.apache.ignite.ml.math.StorageConstants.RANDOM_ACCESS_MODE;

/**
 * XGBoost model composition.
 */
public class XGModelComposition implements Model<HashMap<String, Double>, Double> {
    /** Dictionary used for matching feature names and indexes. */
    private final Map<String, Integer> dict;

    /** Composition of decision trees. */
    private ModelsComposition modelsComposition;

    /**
     * Constructs a new instance of composition of models.
     *
     * @param models Basic models.
     */
    public XGModelComposition(Map<String, Integer> dict, List<DecisionTreeNode> models) {
        this.dict = dict;
        this.modelsComposition = new ModelsComposition(models, new XGModelPredictionsAggregator());
    }

    /** {@inheritDoc} */
    @Override public Double apply(HashMap<String, Double> map) {
        return modelsComposition.apply(toVector(map));
    }

    /** */
    public Map<String, Integer> getDict() {
        return dict;
    }

    /** */
    public ModelsComposition getModelsComposition() {
        return modelsComposition;
    }

    /** */
    public void setModelsComposition(ModelsComposition modelsComposition) {
        this.modelsComposition = modelsComposition;
    }

    /**
     * Converts hash map into sparse vector using dictionary.
     *
     * @param input Hash map with pairs of feature name and feature value.
     * @return Sparse vector.
     */
    private Vector toVector(Map<String, Double> input) {
        Vector inputVector = new SparseVector(dict.size(), RANDOM_ACCESS_MODE);
        for (int i = 0; i < dict.size(); i++)
            inputVector.set(i, Double.NaN);

        for (Map.Entry<String, Double> feature : input.entrySet()) {
            Integer idx = dict.get(feature.getKey());

            if (idx != null)
                inputVector.set(idx, feature.getValue());

        }

        return inputVector;
    }

    /**
     * XG model predictions aggregator.
     */
    private static class XGModelPredictionsAggregator implements PredictionsAggregator {
        /** {@inheritDoc} */
        @Override public Double apply(double[] predictions) {
            double res = 0;

            for (double prediction : predictions)
                res += prediction;

            return (1.0 / (1.0 + Math.exp(-res)));
        }
    }
}