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

import java.util.ArrayList;
import org.apache.ignite.ml.tree.TreeFilter;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.TreeDataIndex;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;

/**
 * Meas squared error (variance) impurity measure calculator.
 */
public class MSEImpurityMeasureCalculator implements ImpurityMeasureCalculator<MSEImpurityMeasure> {
    /** */
    private static final long serialVersionUID = 288747414953756824L;

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public StepFunction<MSEImpurityMeasure>[] calculate(DecisionTreeData data, TreeFilter filter) {
        TreeDataIndex index = data.index();
        if (index.rowsCount() > 0) {
            StepFunction<MSEImpurityMeasure>[] res = new StepFunction[index.columnsCount()];

            for (int col = 0; col < res.length; col++) {
                ArrayList<Double> x = new ArrayList<>();
                ArrayList<MSEImpurityMeasure> y = new ArrayList<>();

                x.add(Double.NEGATIVE_INFINITY);

                double leftY = 0;
                double leftY2 = 0;
                double rightY = 0;
                double rightY2 = 0;

                int rightSize = 0;
                for (int i = 0; i < index.rowsCount(); i++) {
                    double[] vec = index.featuresInSortedOrder(i, col);
                    if (!filter.test(vec))
                        continue;

                    rightY += index.labelInSortedOrder(i, col);
                    rightY2 += Math.pow(index.labelInSortedOrder(i, col), 2);
                    rightSize++;
                }

                int size = 0;
                int lastI = 0;
                for (int i = 0; i <= index.rowsCount(); i++) {
                    if (i < index.rowsCount()) {
                        double[] vec = index.featuresInSortedOrder(i, col);
                        if (!filter.test(vec))
                            continue;
                    }

                    if (size > 0) {
                        leftY += index.labelInSortedOrder(lastI, col);
                        leftY2 += Math.pow(index.labelInSortedOrder(lastI, col), 2);

                        rightY -= index.labelInSortedOrder(lastI, col);
                        rightY2 -= Math.pow(index.labelInSortedOrder(lastI, col), 2);
                    }

                    if (size < rightSize)
                        x.add(index.featureInSortedOrder(i, col));

                    y.add(new MSEImpurityMeasure(
                        leftY, leftY2, size, rightY, rightY2, rightSize - size
                    ));

                    lastI = i;
                    size++;
                }

                res[col] = new StepFunction<>(x, y, MSEImpurityMeasure.class);
            }

            return res;
        }

        return null;
    }
}
