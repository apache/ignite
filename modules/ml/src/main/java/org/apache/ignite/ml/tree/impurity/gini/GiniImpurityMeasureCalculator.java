/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
     * @param useIdx Use index while calculate.
     */
    public GiniImpurityMeasureCalculator(Map<Double, Integer> lbEncoder, boolean useIdx) {
        super(useIdx);
        this.lbEncoder = lbEncoder;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public StepFunction<GiniImpurityMeasure>[] calculate(DecisionTreeData data, TreeFilter filter, int depth) {
        TreeDataIndex idx = null;
        boolean canCalculate = false;

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

            StepFunction<GiniImpurityMeasure>[] res = new StepFunction[colsCnt];

            long right[] = new long[lbEncoder.size()];
            for (int i = 0; i < rowsCnt; i++) {
                double lb = getLabelValue(data, idx, 0, i);
                right[getLabelCode(lb)]++;
            }

            for (int col = 0; col < res.length; col++) {
                if (!useIdx)
                    data.sort(col);

                double[] x = new double[rowsCnt + 1];
                GiniImpurityMeasure[] y = new GiniImpurityMeasure[rowsCnt + 1];

                long[] left = new long[lbEncoder.size()];
                long[] rightCp = Arrays.copyOf(right, right.length);

                int xPtr = 0, yPtr = 0;
                x[xPtr++] = Double.NEGATIVE_INFINITY;
                y[yPtr++] = new GiniImpurityMeasure(
                    Arrays.copyOf(left, left.length),
                    Arrays.copyOf(rightCp, rightCp.length)
                );

                for (int i = 0; i < rowsCnt; i++) {
                    double lb = getLabelValue(data, idx, col, i);
                    left[getLabelCode(lb)]++;
                    rightCp[getLabelCode(lb)]--;

                    double featureVal = getFeatureValue(data, idx, col, i);
                    if (i < (rowsCnt - 1) && getFeatureValue(data, idx, col, i + 1) == featureVal)
                        continue;

                    x[xPtr++] = featureVal;
                    y[yPtr++] = new GiniImpurityMeasure(
                        Arrays.copyOf(left, left.length),
                        Arrays.copyOf(rightCp, rightCp.length)
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
