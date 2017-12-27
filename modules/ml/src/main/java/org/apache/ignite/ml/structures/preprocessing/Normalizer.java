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

package org.apache.ignite.ml.structures.preprocessing;

import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.structures.Dataset;
import org.apache.ignite.ml.structures.DatasetRow;

/** Data preprcessing step which scales features according normalization algorithms. */
public class Normalizer {
    /**
     * Scales features in dataset with MiniMax algorithm x'=(x-MIN[X])/(MAX[X]-MIN[X]). This is an in-place operation.
     * <p>
     * NOTE: Complexity 2*N^2.
     * </p>
     * @param ds The given dataset.
     * @return Transformed dataset.
     */
    public static Dataset normalizeWithMiniMax(Dataset ds) {
        int colSize = ds.colSize();
        double[] mins = new double[colSize];
        double[] maxs = new double[colSize];

        int rowSize = ds.rowSize();
        DatasetRow[] data = ds.data();
        for (int j = 0; j < colSize; j++) {
            double maxInCurrCol = Double.MIN_VALUE;
            double minInCurrCol = Double.MAX_VALUE;

            for (int i = 0; i < rowSize; i++) {
                double e = data[i].features().get(j);
                maxInCurrCol = Math.max(e, maxInCurrCol);
                minInCurrCol = Math.min(e, minInCurrCol);
            }

            mins[j] = minInCurrCol;
            maxs[j] = maxInCurrCol;
        }

        for (int j = 0; j < colSize; j++) {
            double div = maxs[j] - mins[j];

            for (int i = 0; i < rowSize; i++) {
                double oldVal = data[i].features().get(j);
                double newVal = (oldVal - mins[j]) / div;
                // x'=(x-MIN[X])/(MAX[X]-MIN[X])
                data[i].features().set(j, newVal);
            }
        }

        return ds;
    }

    /**
     * Scales features in dataset with Z-Normalization algorithm x'=(x-M[X])/\sigma [X]. This is an in-place operation.
     *
     * @param ds The given dataset.
     * @return Transformed dataset.
     */
    public static Dataset normalizeWithZNormalization(Dataset ds) {
        throw new UnsupportedOperationException("Z-normalization is not supported yet");
    }
}
