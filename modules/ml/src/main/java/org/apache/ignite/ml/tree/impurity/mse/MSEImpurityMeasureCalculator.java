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
import org.apache.ignite.lang.IgniteBiPredicate;
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
    @Override public StepFunction<MSEImpurityMeasure>[] calculate(DecisionTreeData data,
        IgniteBiPredicate<Integer, Double> featuresFilter) {

        TreeDataIndex index = data.index();
        if (index.rowsCount() > 0) {
            StepFunction<MSEImpurityMeasure>[] res = new StepFunction[index.featuresCount()];

            for (int col = 0; col < res.length; col++) {
                ArrayList<Double> x = new ArrayList<>();
                ArrayList<MSEImpurityMeasure> y = new ArrayList<>();
                x.add(Double.NEGATIVE_INFINITY);

                double leftY = 0;
                double leftY2 = 0;
                double rightY = 0;
                double rightY2 = 0;

                int rightCount = 0;
                for (int i = 0; i < index.rowsCount(); i++) {
                    double feature = index.featureInSortedOrder(i, col);
                    if (!featuresFilter.apply(col, feature))
                        continue;

                    double label = index.labelInSortedOrder(i, col);
                    rightY += label;
                    rightY2 += Math.pow(label, 2);
                    rightCount++;
                }

                int size = 0;
                int lastIth = 0;
                for (int leftSize = 0; leftSize <= index.rowsCount(); leftSize++) {
                    if (leftSize < index.rowsCount()) {
                        double feature = index.featureInSortedOrder(leftSize, col);
                        if (!featuresFilter.apply(col, feature))
                            continue;
                    }

                    if (leftSize > 0) {
                        double label = index.labelInSortedOrder(lastIth, col);
                        double label2 = Math.pow(label, 2);

                        leftY += label;
                        leftY2 += label2;

                        rightY -= label;
                        rightY2 -= label2;
                    }

                    if (leftSize < index.rowsCount())
                        x.add(index.featureInSortedOrder(leftSize, col));

                    y.add(new MSEImpurityMeasure(
                        leftY, leftY2, size, rightY, rightY2, rightCount - size
                    ));

                    size++;
                    lastIth = leftSize;
                }

                res[col] = new StepFunction<>(x, y, MSEImpurityMeasure.class);
            }

            return res;
        }

        return null;
    }
}
