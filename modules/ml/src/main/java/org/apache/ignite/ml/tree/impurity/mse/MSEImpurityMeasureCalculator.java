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

import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.util.SimpleStepFunctionCompressor;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;
import org.apache.ignite.ml.tree.impurity.util.StepFunctionCompressor;

/**
 * Meas squared error (variance) impurity measure calculator.
 */
public class MSEImpurityMeasureCalculator implements ImpurityMeasureCalculator<MSEImpurityMeasure> {
    /** */
    private static final long serialVersionUID = 288747414953756824L;

    /** Step function compressor. */
    private final StepFunctionCompressor<MSEImpurityMeasure> compressor;

    /**
     * Constructs a new instance of MSE impurity measure calculator.
     */
    public MSEImpurityMeasureCalculator() {
        this(new SimpleStepFunctionCompressor<>());
    }

    /**
     * Constructs a new instance of MSE impurity measure calculator.
     *
     * @param compressor Step function compressor.
     */
    public MSEImpurityMeasureCalculator(StepFunctionCompressor<MSEImpurityMeasure> compressor) {
        this.compressor = compressor;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public StepFunction<MSEImpurityMeasure>[] calculate(DecisionTreeData data) {
        if (data.getFeatures().length > 0) {
            StepFunction<MSEImpurityMeasure>[] res = new StepFunction[data.getFeatures()[0].length];

            for (int col = 0; col < res.length; col++) {
                data.sort(col);

                double[] x = new double[data.getFeatures().length + 1];
                MSEImpurityMeasure[] y = new MSEImpurityMeasure[data.getFeatures().length + 1];

                x[0] = Double.NEGATIVE_INFINITY;

                for (int leftSize = 0; leftSize <= data.getFeatures().length; leftSize++) {
                    double leftY = 0;
                    double leftY2 = 0;
                    double rightY = 0;
                    double rightY2 = 0;

                    for (int i = 0; i < leftSize; i++) {
                        leftY += data.getLabels()[i];
                        leftY2 += Math.pow(data.getLabels()[i], 2);
                    }

                    for (int i = leftSize; i < data.getFeatures().length; i++) {
                        rightY += data.getLabels()[i];
                        rightY2 += Math.pow(data.getLabels()[i], 2);
                    }

                    if (leftSize < data.getFeatures().length)
                        x[leftSize + 1] = data.getFeatures()[leftSize][col];

                    y[leftSize] = new MSEImpurityMeasure(leftY, leftY2, leftSize, rightY, rightY2, data.getFeatures().length - leftSize);
                }

                res[col] = compressor.compress(new StepFunction<>(x, y));
            }

            return res;
        }

        return null;
    }
}
