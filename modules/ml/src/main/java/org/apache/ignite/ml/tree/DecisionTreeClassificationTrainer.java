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
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.gini.GiniImpurityMeasure;
import org.apache.ignite.ml.tree.impurity.gini.GiniImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.StepFunctionCompressor;
import org.apache.ignite.ml.tree.leaf.MostCommonDecisionTreeLeafBuilder;

/**
 * Decision tree classifier based on distributed decision tree trainer that allows to fit trees using row-partitioned
 * dataset.
 */
public class DecisionTreeClassificationTrainer extends DecisionTree<GiniImpurityMeasure> {
    /**
     * Constructs a new decision tree classifier with default impurity function compressor.
     *
     * @param maxDeep Max tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public DecisionTreeClassificationTrainer(int maxDeep, double minImpurityDecrease) {
        this(maxDeep, minImpurityDecrease, null);
    }

    /**
     * Constructs a new instance of decision tree classifier.
     *
     * @param maxDeep Max tree deep.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public DecisionTreeClassificationTrainer(int maxDeep, double minImpurityDecrease,
        StepFunctionCompressor<GiniImpurityMeasure> compressor) {
        super(maxDeep, minImpurityDecrease, compressor, new MostCommonDecisionTreeLeafBuilder());
    }

    /** {@inheritDoc} */
    @Override ImpurityMeasureCalculator<GiniImpurityMeasure> getImpurityMeasureCalculator(
        Dataset<EmptyContext, DecisionTreeData> dataset) {
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
}
