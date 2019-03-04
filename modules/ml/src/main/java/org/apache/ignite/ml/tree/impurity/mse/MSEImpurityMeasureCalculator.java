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

package org.apache.ignite.ml.tree.impurity.mse;

import org.apache.ignite.ml.tree.TreeFilter;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.TreeDataIndex;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;

/**
 * Meas squared error (variance) impurity measure calculator.
 */
public class MSEImpurityMeasureCalculator extends ImpurityMeasureCalculator<MSEImpurityMeasure> {
    /** */
    private static final long serialVersionUID = 288747414953756824L;

    /**
     * Constructs an instance of MSEImpurityMeasureCalculator.
     *
     * @param useIdx Use index while calculate.
     */
    public MSEImpurityMeasureCalculator(boolean useIdx) {
        super(useIdx);
    }

    /** {@inheritDoc} */
    @Override public StepFunction<MSEImpurityMeasure>[] calculate(DecisionTreeData data, TreeFilter filter, int depth) {
        TreeDataIndex idx = null;
        boolean canCalculate;

        if (useIdx) {
            idx = data.createIndexByFilter(depth, filter);
            canCalculate = idx.rowsCount() > 0;
        }
        else {
            data = data.filter(filter);
            canCalculate = data.getFeatures().length > 0;
        }

        if (canCalculate) {
            int rowsCnt = rowsCount(data, idx);
            int colsCnt = columnsCount(data, idx);

            @SuppressWarnings("unchecked")
            StepFunction<MSEImpurityMeasure>[] res = new StepFunction[colsCnt];

            double rightYOriginal = 0;
            double rightY2Original = 0;
            for (int i = 0; i < rowsCnt; i++) {
                double lbVal = getLabelValue(data, idx, 0, i);

                rightYOriginal += lbVal;
                rightY2Original += Math.pow(lbVal, 2);
            }

            for (int col = 0; col < res.length; col++) {
                if (!useIdx)
                    data.sort(col);

                double[] x = new double[rowsCnt + 1];
                MSEImpurityMeasure[] y = new MSEImpurityMeasure[rowsCnt + 1];

                x[0] = Double.NEGATIVE_INFINITY;

                double leftY = 0;
                double leftY2 = 0;
                double rightY = rightYOriginal;
                double rightY2 = rightY2Original;

                int leftSize = 0;
                for (int i = 0; i <= rowsCnt; i++) {
                    if (leftSize > 0) {
                        double lblVal = getLabelValue(data, idx, col, i - 1);

                        leftY += lblVal;
                        leftY2 += Math.pow(lblVal, 2);

                        rightY -= lblVal;
                        rightY2 -= Math.pow(lblVal, 2);
                    }

                    if (leftSize < rowsCnt)
                        x[leftSize + 1] = getFeatureValue(data, idx, col, i);

                    y[leftSize] = new MSEImpurityMeasure(
                        leftY, leftY2, leftSize, rightY, rightY2, rowsCnt - leftSize
                    );

                    leftSize++;
                }

                res[col] = new StepFunction<>(x, y);
            }

            return res;
        }

        return null;
    }
}
