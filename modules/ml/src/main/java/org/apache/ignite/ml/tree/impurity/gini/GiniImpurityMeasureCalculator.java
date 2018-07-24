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

import java.util.ArrayList;
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
    @Override public StepFunction<GiniImpurityMeasure>[] calculate(DecisionTreeData data, TreeFilter filter) {
        TreeDataIndex index = data.index();
        if (index.rowsCount() > 0) {
            StepFunction<GiniImpurityMeasure>[] res = new StepFunction[index.columnsCount()];

            for (int col = 0; col < res.length; col++) {
                ArrayList<Double> x = new ArrayList<>();
                ArrayList<GiniImpurityMeasure> y = new ArrayList<>();
//                double[] x = new double[index.rowsCount() + 1];
//                GiniImpurityMeasure[] y = new GiniImpurityMeasure[index.rowsCount() + 1];

                int xPtr = 0, yPtr = 0;

                long[] left = new long[lbEncoder.size()];
                long[] right = new long[lbEncoder.size()];

                for (int i = 0; i < index.rowsCount(); i++) {
                    double[] featureVector = index.featuresInSortedOrder(i, col);
                    if(!filter.test(featureVector))
                        continue;

                    double label = index.labelInSortedOrder(i, col);
                    right[getLabelCode(label)]++;
                }

                x.add(Double.NEGATIVE_INFINITY);
                y.add(new GiniImpurityMeasure(
                    Arrays.copyOf(left, left.length),
                    Arrays.copyOf(right, right.length)
                ));
//                x[xPtr++] = Double.NEGATIVE_INFINITY;
//                y[yPtr++] = new GiniImpurityMeasure(
//                    Arrays.copyOf(left, left.length),
//                    Arrays.copyOf(right, right.length)
//                );

                int lastRowId = 0;
                for (int i = 0; i < index.rowsCount(); i++) {
                    double[] featureVector = index.featuresInSortedOrder(i, col);
                    if(!filter.test(featureVector))
                        continue;

                    double label = index.labelInSortedOrder(i, col);
                    left[getLabelCode(label)]++;
                    right[getLabelCode(label)]--;

                    if (i < (index.rowsCount() - 1) && index.featureInSortedOrder(lastRowId, col) == index.featureInSortedOrder(i, col))
                        continue;

                    x.add(index.featureInSortedOrder(i, col));
                    y.add(new GiniImpurityMeasure(
                        Arrays.copyOf(left, left.length),
                        Arrays.copyOf(right, right.length)
                    ));

                    lastRowId = i;
                }

                res[col] = new StepFunction<>(x, y, GiniImpurityMeasure.class);
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
