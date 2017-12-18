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

package org.apache.ignite.ml.nn;

import java.util.Random;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.util.Utils;

public class SimpleMLPLocalBatchTrainerInput implements MLPLocalBatchTrainerInput {
    private final MLP mlp;
    private Matrix inputs;
    private final Matrix outputs;
    private int batchSize;

    public SimpleMLPLocalBatchTrainerInput(MLPArchitecture conf, Random rnd, Matrix inputs, Matrix outputs, int batchSize) {
        this.mlp = new MLP(conf, new RandomInitializer(rnd));
        this.inputs = inputs;
        this.outputs = outputs;
        this.batchSize = batchSize;
    }

    @Override public IgniteBiTuple<Matrix, Matrix> getBatch() {
        int inputRowSize = inputs.rowSize();
        int outputRowSize = outputs.rowSize();

        Matrix vectors = new DenseLocalOnHeapMatrix(inputRowSize, batchSize);
        Matrix labels = new DenseLocalOnHeapMatrix(outputRowSize, batchSize);

        int[] samples = Utils.selectKDistinct(inputs.columnSize(), batchSize);

        for (int i = 0; i < batchSize; i++) {
            vectors.assignColumn(i, inputs.getCol(samples[i]));
            labels.assignColumn(i, outputs.getCol(samples[i]));
        }

        return new IgniteBiTuple<>(vectors, labels);
    }

    @Override public MLP mlp() {
        return mlp;
    }
}
