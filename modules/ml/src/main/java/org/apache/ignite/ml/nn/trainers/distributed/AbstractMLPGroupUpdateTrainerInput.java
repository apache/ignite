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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.LocalBatchTrainerInput;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.updaters.ParameterUpdateCalculator;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.GroupTrainerInput;

public abstract class AbstractMLPGroupUpdateTrainerInput<U> implements GroupTrainerInput<Void>,LocalBatchTrainerInput<MultilayerPerceptron> {
    private final int networksCount;
    private final int globalSteps;
    private final int syncRate;
    private final IgniteFunction<List<U>, U> allUpdatesReducer;
    private final IgniteFunction<List<U>, U> oneTrainingUpdatesReducer;
    private final ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator;

    public AbstractMLPGroupUpdateTrainerInput(int networksCnt, int globalSteps, int syncRate, IgniteFunction<List<U>, U> allUpdatesReducer,
        IgniteFunction<List<U>, U> oneTrainingUpdatesReducer, ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator) {
        this.networksCount = networksCnt;
        this.globalSteps = globalSteps;
        this.syncRate = syncRate;
        this.allUpdatesReducer = allUpdatesReducer;
        this.oneTrainingUpdatesReducer = oneTrainingUpdatesReducer;
        this.updateCalculator = updateCalculator;
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> initialKeys(UUID trainingUUID) {
        int nt = networksCount;
        return () -> MLPCache.allKeys(nt, trainingUUID);
    }

    public int globalSteps() {
        return globalSteps;
    }

    public int syncRate() {
        return syncRate;
    }

    public IgniteFunction<List<U>, U> allUpdatesReducer() {
        return allUpdatesReducer;
    }

    public int trainingsCount() {
        return networksCount;
    }

    public IgniteFunction<List<U>, U> oneTrainingUpdatesReducer() {
        return oneTrainingUpdatesReducer;
    }

    public ParameterUpdateCalculator<MultilayerPerceptron, U> updateCalculator() {
        return updateCalculator;
    }
}
