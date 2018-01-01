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

import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.LocalBatchTrainerInput;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.GroupTrainerInput;

/**
 * Abstract class for {@link MLPGroupUpdateTrainer} inputs.
 */
public abstract class AbstractMLPGroupUpdateTrainerInput implements GroupTrainerInput<Void>, LocalBatchTrainerInput<MultilayerPerceptron> {
    /**
     * Count of networks to be trained in parallel.
     */
    private final int networksCnt;

    /**
     * Construct instance of this class with given parameters.
     *
     * @param networksCnt Count of networks to be trained in parallel.
     */
    public AbstractMLPGroupUpdateTrainerInput(int networksCnt) {
        this.networksCnt = networksCnt;
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> initialKeys(UUID trainingUUID) {
        final int nt = networksCnt; // IMPL NOTE intermediate variable is intended to have smaller lambda
        return () -> MLPCache.allKeys(nt, trainingUUID);
    }

    /**
     * Get count of networks to be trained in parallel.
     *
     * @return Count of networks.
     */
    public int trainingsCount() {
        return networksCnt;
    }
}
