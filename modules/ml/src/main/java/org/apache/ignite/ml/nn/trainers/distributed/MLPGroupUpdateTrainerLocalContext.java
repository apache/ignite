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

import java.util.List;
import java.util.UUID;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trainers.group.chain.HasTrainingUUID;

/**
 * Local context for {@link MLPGroupUpdateTrainer}.
 *
 * @param <U> Type of updates on which training is done.
 */
public class MLPGroupUpdateTrainerLocalContext<U> implements HasTrainingUUID {
    /**
     * UUID of training.
     */
    private final UUID trainingUUID;

    /**
     * Maximal number of global steps.
     */
    private final int globalStepsMaxCnt;

    /**
     * Reducer used to reduce updates resulted from each parallel training.
     */
    private final IgniteFunction<List<U>, U> allUpdatesReducer;

    /**
     * Count of networks to be trained in parallel.
     */
    private final int parallelTrainingsCnt;

    /**
     * Current global step of {@link MLPGroupUpdateTrainer}.
     */
    private int curStep;

    /** Create multilayer perceptron group update trainer local context. */
    public MLPGroupUpdateTrainerLocalContext(UUID trainingUUID, int globalStepsMaxCnt,
        IgniteFunction<List<U>, U> allUpdatesReducer, int parallelTrainingsCnt) {
        this.trainingUUID = trainingUUID;
        this.globalStepsMaxCnt = globalStepsMaxCnt;
        this.allUpdatesReducer = allUpdatesReducer;
        this.parallelTrainingsCnt = parallelTrainingsCnt;
        curStep = 0;
    }

    /** {@inheritDoc} */
    @Override public UUID trainingUUID() {
        return trainingUUID;
    }

    /**
     * Get global steps max count.
     *
     * @return Global steps max count.
     */
    public int globalStepsMaxCount() {
        return globalStepsMaxCnt;
    }

    /**
     * Get reducer used to reduce updates resulted from each parallel training.
     *
     * @return Reducer used to reduce updates resulted from each parallel training.
     */
    public IgniteFunction<List<U>, U> allUpdatesReducer() {
        return allUpdatesReducer;
    }

    /**
     * Get count of networks to be trained in parallel.
     *
     * @return Count of networks to be trained in parallel.
     */
    public int parallelTrainingsCnt() {
        return parallelTrainingsCnt;
    }

    /**
     * Get current global step.
     *
     * @return Current global step.
     */
    public int currentStep() {
        return curStep;
    }

    /**
     * Increment current global step.
     *
     * @return This object.
     */
    public MLPGroupUpdateTrainerLocalContext<U> incrementCurrentStep() {
        curStep++;

        return this;
    }
}
