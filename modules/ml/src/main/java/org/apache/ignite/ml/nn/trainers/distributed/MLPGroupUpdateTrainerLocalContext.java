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

public class MLPGroupUpdateTrainerLocalContext<U> implements HasTrainingUUID {
    /**
     * UUID of training.
     */
    private final UUID trainingUUID;

    /**
     * Maximal number of global steps.
     */
    private final int globalStepsMaxCount;

//    /**
//     * Synchronize between nodes every syncRate steps.
//     */
//    private final int syncRate;

    private final IgniteFunction<List<U>, U> allUpdatesReducer;

    private final int parallelTrainingsCnt;

    private int curStep;

//    private final IgniteFunction<List<U>, U> modelUpdatesReducer;

    public MLPGroupUpdateTrainerLocalContext(UUID trainingUUID, int globalStepsMaxCount, IgniteFunction<List<U>, U> allUpdatesReducer, int parallelTrainingsCnt) {
        this.trainingUUID = trainingUUID;
        this.globalStepsMaxCount = globalStepsMaxCount;
        this.allUpdatesReducer = allUpdatesReducer;
        this.parallelTrainingsCnt = parallelTrainingsCnt;
        curStep = 0;
    }

    /** {@inheritDoc} */
    @Override public UUID trainingUUID() {
        return trainingUUID;
    }

    public int globalStepsMaxCount() {
        return globalStepsMaxCount;
    }

    public IgniteFunction<List<U>, U> allUpdatesReducer() {
        return allUpdatesReducer;
    }

    public int parallelTrainingsCnt() {
        return parallelTrainingsCnt;
    }

    public int currentStep() {
        return 0;
    }

    public MLPGroupUpdateTrainerLocalContext<U> incrementCurrentStep() {
        curStep++;

        return this;
    }
}
