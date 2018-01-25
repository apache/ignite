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

package org.apache.ignite.examples.ml.dlc.transformer;

import org.apache.ignite.ml.dlc.dataset.part.DLCLabeledDatasetPartitionRecoverable;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/**
 * Transformer which transforms dataset recoverable data to algorithm specific recoverable data by supplementing column
 * with value "1".
 */
public class AlgorithmSpecificRecoverableDataTransformer
    implements IgniteFunction<DLCLabeledDatasetPartitionRecoverable, AlgorithmSpecificRecoverableData> {
    /** */
    private static final long serialVersionUID = 2292486552367922497L;

    /** {@inheritDoc} */
    @Override public AlgorithmSpecificRecoverableData apply(DLCLabeledDatasetPartitionRecoverable recoverable) {
        int rows = recoverable.getRows();
        int cols = recoverable.getCols();

        // Add first column with values equal to 1.0.
        double[] supplementedMatrix = new double[rows * (cols + 1)];
        for (int i = 0; i < rows; i++)
            supplementedMatrix[i] = 1.0;
        System.arraycopy(recoverable.getFeatures(), 0, supplementedMatrix, rows, rows * cols);

        return new AlgorithmSpecificRecoverableData(supplementedMatrix, rows, cols + 1, recoverable.getLabels());
    }
}
