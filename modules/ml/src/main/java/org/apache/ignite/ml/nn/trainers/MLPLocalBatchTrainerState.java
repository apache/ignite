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

package org.apache.ignite.ml.nn.trainers;

/**
 * State of {@link MLPLocalBatchTrainer}.
 */
public class MLPLocalBatchTrainerState {
    /**
     * Current iteration of MLP trainer.
     */
    private int curIteration;

    /**
     * Current error of trained model.
     */
    private double currError;

    /**
     * Get current error of trained model.
     *
     * @return Current error of trained model.
     */
    public double currentError() {
        return currError;
    }

    /**
     * Set current error of trained model.
     *
     * @param currError new value of current error.
     * @return This object.
     */
    MLPLocalBatchTrainerState setCurrentError(double currError) {
        this.currError = currError;
        return this;
    }

    /**
     * Get current trainer iteration.
     *
     * @return Current trainer iteration.
     */
    public int currentIteration() {
        return curIteration;
    }

    /**
     * Set current trainer iteration.
     *
     * @param curIteration new value of current iteration.
     * @return This object.
     */
    MLPLocalBatchTrainerState setCurrentIteration(int curIteration) {
        this.curIteration = curIteration;

        return this;
    }
}
