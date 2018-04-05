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

package org.apache.ignite.ml.tree;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.tree.impurity.gini.GiniImpurityMeasure;
import org.apache.ignite.ml.tree.impurity.gini.GiniImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;

/**
 * Decision tree classifier based on distributed decision tree trainer that allows to fit trees using row-partitioned
 * dataset.
 */
public class DecisionTreeClassifier extends DecisionTree<GiniImpurityMeasure> {
    /**
     * Constructs a new instance of decision tree classifier.
     *
     * @param maxDeep Max tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public DecisionTreeClassifier(int maxDeep, double minImpurityDecrease) {
        super(maxDeep, minImpurityDecrease);
    }

    /** {@inheritDoc} */
    @Override TreeLeafNode createLeafNode(Dataset<EmptyContext, SimpleLabeledDatasetData> dataset,
        Predicate<double[]> pred) {
        Map<Double, Integer> cnt = dataset.compute(part -> {

            if (part.getFeatures() != null) {
                Map<Double, Integer> map = new HashMap<>();

                double[][] features = convert(part.getFeatures(), part.getRows());
                double[] labels = part.getLabels();

                for (int i = 0; i < features.length; i++) {
                    if (pred.test(features[i])) {
                        double lb = labels[i];
                        if (map.containsKey(lb))
                            map.put(lb, map.get(lb) + 1);
                        else
                            map.put(lb, 1);
                    }
                }

                return map;
            }

            return null;

        }, this::reduce);

        double bestVal = 0;
        int bestCnt = -1;

        for (Map.Entry<Double, Integer> e : cnt.entrySet()) {
            if (e.getValue() > bestCnt) {
                bestCnt = e.getValue();
                bestVal = e.getKey();
            }
        }

        return new TreeLeafNode(bestVal);
    }

    /** {@inheritDoc} */
    @Override ImpurityMeasureCalculator<GiniImpurityMeasure> getImpurityMeasureCalculator(Dataset<EmptyContext, SimpleLabeledDatasetData> dataset) {
        Set<Double> labels = dataset.compute(part -> {

            if (part.getLabels() != null) {
                Set<Double> list = new HashSet<>();

                for (double lb : part.getLabels())
                    list.add(lb);

                return list;
            }

            return null;

        }, (a, b) -> {
            if (a == null)
                return b;
            else if (b == null)
                return a;
            else {
                a.addAll(b);
                return a;
            }
        });

        Map<Double, Integer> encoder = new HashMap<>();
        int idx = 0;
        for (Double lb : labels)
            encoder.put(lb, idx++);

        return new GiniImpurityMeasureCalculator(encoder);
    }

    private Map<Double, Integer> reduce(Map<Double, Integer> a, Map<Double, Integer> b) {
        if (a == null)
            return b;
        else if (b == null)
            return a;
        else {
            for (Map.Entry<Double, Integer> e : b.entrySet()) {
                if (a.containsKey(e.getKey()))
                    a.put(e.getKey(), a.get(e.getKey()) + e.getValue());
                else
                    a.put(e.getKey(), e.getValue());
            }
            return a;
        }
    }
}
