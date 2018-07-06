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

package org.apache.ignite.ml.tree.impurity.gini;

import java.util.Arrays;
import java.util.Map;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;

/**
 * Gini impurity measure calculator.
 */
public class GiniImpurityMeasureCalculator implements ImpurityMeasureCalculator<GiniImpurityMeasure> {
    /** */
    private static final long serialVersionUID = -522995134128519679L;

    /** Label encoder which defines integer value for every label class. */
    private final Map<Double, Integer> lbEncoder;

    /**
     * Constructs a new instance of Gini impurity measure calculator.
     *
     * @param lbEncoder Label encoder which defines integer value for every label class.
     */
    public GiniImpurityMeasureCalculator(Map<Double, Integer> lbEncoder) {
        this.lbEncoder = lbEncoder;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public StepFunction<GiniImpurityMeasure>[] calculate(DecisionTreeData data) {
        double[][] features = data.getFeatures();
        double[] labels = data.getLabels();

        if (features.length > 0) {
            StepFunction<GiniImpurityMeasure>[] res = new StepFunction[features[0].length];

            for (int col = 0; col < res.length; col++) {
                data.sort(col);

                double[] x = new double[features.length + 1];
                GiniImpurityMeasure[] y = new GiniImpurityMeasure[features.length + 1];

                int xPtr = 0, yPtr = 0;

                long[] left = new long[lbEncoder.size()];
                long[] right = new long[lbEncoder.size()];

                for (int i = 0; i < labels.length; i++)
                    right[getLabelCode(labels[i])]++;

                x[xPtr++] = Double.NEGATIVE_INFINITY;
                y[yPtr++] = new GiniImpurityMeasure(
                    Arrays.copyOf(left, left.length),
                    Arrays.copyOf(right, right.length)
                );

                for (int i = 0; i < features.length; i++) {
                    left[getLabelCode(labels[i])]++;
                    right[getLabelCode(labels[i])]--;

                    if (i < (features.length - 1) && features[i + 1][col] == features[i][col])
                        continue;

                    x[xPtr++] = features[i][col];
                    y[yPtr++] = new GiniImpurityMeasure(
                        Arrays.copyOf(left, left.length),
                        Arrays.copyOf(right, right.length)
                    );
                }

                res[col] = new StepFunction<>(Arrays.copyOf(x, xPtr), Arrays.copyOf(y, yPtr));
            }

            return res;
        }

        return null;
    }

    /**
     * Returns label code.
     *
     * @param lb Label.
     * @return Label code.
     */
    int getLabelCode(double lb) {
        Integer code = lbEncoder.get(lb);

        assert code != null : "Can't find code for label " + lb;

        return code;
    }
}
