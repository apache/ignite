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
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.trainers.group.Metaoptimizer;

public class MLPMetaoptimizer<P> implements Metaoptimizer<MLPGroupUpdateTrainerLocalContext, MLPGroupUpdateTrainingLoopData<P>, P, P, P, P> {
    @Override public IgniteFunction<List<P>, P> initialReducer() {
        return null;
    }

    @Override public P locallyProcessInitData(P data, MLPGroupUpdateTrainerLocalContext locCtx) {
        return data;
    }

    @Override public IgniteFunction<P, P> distributedPostprocessor() {
        return null;
    }

    @Override public IgniteFunction<List<P>, P> postProcessReducer() {
        return null;
    }

    @Override public P localProcessor(P input, MLPGroupUpdateTrainerLocalContext locCtx) {
        locCtx.incrementCurrentStep();

        return input;
    }

    @Override public boolean shouldContinue(P input, MLPGroupUpdateTrainerLocalContext locCtx) {
        return input != null && locCtx.currentStep() < locCtx.globalStepsMaxCount();
    }
}
