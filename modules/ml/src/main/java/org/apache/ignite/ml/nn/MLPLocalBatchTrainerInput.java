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

import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.nn.trainers.MLPLocalBatchTrainer;

/**
 * Interface for classes containing input parameters for {@link MLPLocalBatchTrainer}
 */
public interface MLPLocalBatchTrainerInput {
    /**
     * Get next batch in form of matrix of inputs and matrix of outputs.
     * @return Next batch.
     */
    IgniteBiTuple<Matrix, Matrix> getBatch();

    /**
     * MLP to train.
     * @return MLP to train.
     */
    MLP mlp();
}
