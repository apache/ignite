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
import org.apache.ignite.ml.tree.impurity.util.SimpleStepFunctionCompressor;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;
import org.apache.ignite.ml.tree.impurity.util.StepFunctionCompressor;

/**
 * Gini impurity measure calculator.
 */
public class GiniImpurityMeasureCalculator implements ImpurityMeasureCalculator<GiniImpurityMeasure> {
    /** */
    private static final long serialVersionUID = -522995134128519679L;

    /** Label encoder which defines integer value for every label class.  */
    private final Map<Double, Integer> lbEncoder;

    /** Step function compressor. */
    private final StepFunctionCompressor<GiniImpurityMeasure> compressor;

    /**
     * Constructs a new instance of Gini impurity measure calculator.
     *
     * @param lbEncoder Label encoder which defines integer value for every label class.
     */
    public GiniImpurityMeasureCalculator(Map<Double, Integer> lbEncoder) {
        this(lbEncoder, new SimpleStepFunctionCompressor<>());
    }

    /**
     * Constructs a new instance of Gini impurity measure calculator.
     *
     * @param lbEncoder Label encoder which defines integer value for every label class.
     * @param compressor Step function compressor.
     */
    public GiniImpurityMeasureCalculator(Map<Double, Integer> lbEncoder,
        StepFunctionCompressor<GiniImpurityMeasure> compressor) {
        this.lbEncoder = lbEncoder;
        this.compressor = compressor;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public StepFunction<GiniImpurityMeasure>[] calculate(DecisionTreeData data) {
        if (data.getFeatures().length > 0) {
            StepFunction<GiniImpurityMeasure>[] res = new StepFunction[data.getFeatures()[0].length];

            for (int col = 0; col < res.length; col++) {
                data.sort(col);

                double[] x = new double[data.getFeatures().length + 1];
                GiniImpurityMeasure[] y = new GiniImpurityMeasure[data.getFeatures().length + 1];

                int xPtr = 0, yPtr = 0;

                x[xPtr++] = Double.NEGATIVE_INFINITY;

                for (int leftSize = 0; leftSize <= data.getFeatures().length; leftSize++) {
                    if (leftSize > 0 && leftSize < data.getFeatures().length && data.getFeatures()[leftSize][col] == data.getFeatures()[leftSize - 1][col])
                        continue;

                    long[] left = new long[lbEncoder.size()];
                    long[] right = new long[lbEncoder.size()];

                    for (int j = 0; j < leftSize; j++)
                        left[getLabelCode(data.getLabels()[j])]++;

                    for (int j = leftSize; j < data.getLabels().length; j++)
                        right[getLabelCode(data.getLabels()[j])]++;

                    if (leftSize < data.getFeatures().length)
                        x[xPtr++] = data.getFeatures()[leftSize][col];

                    y[yPtr++] = new GiniImpurityMeasure(left, right);
                }

                res[col] = compressor.compress(new StepFunction<>(Arrays.copyOf(x, xPtr), Arrays.copyOf(y, yPtr)));
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
