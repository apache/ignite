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
import org.apache.ignite.ml.tree.TreeFilter;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.TreeDataIndex;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;

/**
 * Gini impurity measure calculator.
 */
public class GiniImpurityMeasureCalculator extends ImpurityMeasureCalculator<GiniImpurityMeasure> {
    /** */
    private static final long serialVersionUID = -522995134128519679L;

    /** Label encoder which defines integer value for every label class. */
    private final Map<Double, Integer> lbEncoder;

    /**
     * Constructs a new instance of Gini impurity measure calculator.
     *
     * @param lbEncoder Label encoder which defines integer value for every label class.
     * @param useIndex Use index while calculate.
     */
    public GiniImpurityMeasureCalculator(Map<Double, Integer> lbEncoder, boolean useIndex) {
        super(useIndex);
        this.lbEncoder = lbEncoder;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public StepFunction<GiniImpurityMeasure>[] calculate(DecisionTreeData data, TreeFilter filter, int depth) {
        TreeDataIndex index = null;
        boolean canCalculate = false;

        if (useIndex) {
            index = data.createIndexByFilter(depth, filter);
            canCalculate = index.rowsCount() > 0;
        }
        else {
            data = data.filter(filter);
            canCalculate = data.getFeatures().length > 0;
        }

        if (canCalculate) {
            int rowsCnt = rowsCount(data, index);
            int colsCnt = columnsCount(data, index);

            StepFunction<GiniImpurityMeasure>[] res = new StepFunction[colsCnt];

            long right[] = new long[lbEncoder.size()];
            for (int i = 0; i < rowsCnt; i++) {
                double lb = getLabelValue(data, index, 0, i);
                right[getLabelCode(lb)]++;
            }

            for (int col = 0; col < res.length; col++) {
                if(!useIndex)
                    data.sort(col);

                double[] x = new double[rowsCnt + 1];
                GiniImpurityMeasure[] y = new GiniImpurityMeasure[rowsCnt + 1];

                long[] left = new long[lbEncoder.size()];
                long[] rightCopy = Arrays.copyOf(right, right.length);

                int xPtr = 0, yPtr = 0;
                x[xPtr++] = Double.NEGATIVE_INFINITY;
                y[yPtr++] = new GiniImpurityMeasure(
                    Arrays.copyOf(left, left.length),
                    Arrays.copyOf(rightCopy, rightCopy.length)
                );

                for (int i = 0; i < rowsCnt; i++) {
                    double lb = getLabelValue(data, index, col, i);
                    left[getLabelCode(lb)]++;
                    rightCopy[getLabelCode(lb)]--;

                    double featureVal = getFeatureValue(data, index, col, i);
                    if (i < (rowsCnt - 1) && getFeatureValue(data, index, col, i + 1) == featureVal)
                        continue;

                    x[xPtr++] = featureVal;
                    y[yPtr++] = new GiniImpurityMeasure(
                        Arrays.copyOf(left, left.length),
                        Arrays.copyOf(rightCopy, rightCopy.length)
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
