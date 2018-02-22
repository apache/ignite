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

package org.apache.ignite.ml.nn.trainers.distributed;

/**
 * Context extracted in distribute phase of training loop step in {@link MLPGroupUpdateTrainer}.
 *
 * @param <U> Type of update.
 */
public class MLPGroupUpdateTrainingContext<U> {
    /**
     * Group training data.
     */
    private final MLPGroupUpdateTrainingData<U> data;

    /**
     * Update produced by previous training loop step.
     */
    private final U previousUpdate;

    /**
     * Construct an instance of this class.
     *
     * @param data Group training data.
     * @param previousUpdate Update produced by previous training loop step.
     */
    public MLPGroupUpdateTrainingContext(MLPGroupUpdateTrainingData<U> data, U previousUpdate) {
        this.data = data;
        this.previousUpdate = previousUpdate;
    }

    /**
     * Get group training data.
     *
     * @return Group training data.
     */
    public MLPGroupUpdateTrainingData<U> data() {
        return data;
    }

    /**
     * Get update produced by previous training loop step.
     *
     * @return Update produced by previous training loop step.
     */
    public U previousUpdate() {
        return previousUpdate;
    }
}
