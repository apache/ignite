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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteSupplier;
import org.apache.ignite.ml.nn.LocalBatchTrainerInput;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.trainers.group.GroupTrainerCacheKey;
import org.apache.ignite.ml.trainers.group.GroupTrainerInput;

public abstract class MLPGroupUpdateTrainerInput implements GroupTrainerInput<Void>,LocalBatchTrainerInput<MultilayerPerceptron> {
    private int networksCount;

    public MLPGroupUpdateTrainerInput(int networksCnt) {
        this.networksCount = networksCnt;
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<Void>>> initialKeys(UUID trainingUUID) {
        int nt = networksCount;
        return () -> IntStream.range(0, nt).mapToObj(idx -> new GroupTrainerCacheKey<Void>(idx, null, trainingUUID));
    }
}
