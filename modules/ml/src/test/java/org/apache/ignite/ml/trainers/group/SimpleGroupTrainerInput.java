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

package org.apache.ignite.ml.trainers.group;

import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.functions.IgniteSupplier;

/** */
class SimpleGroupTrainerInput implements GroupTrainerInput<Double> {
    /** */
    private int limit;
    /** */
    private int eachNumCnt;
    /** */
    private int iterCnt;

    /** */
    SimpleGroupTrainerInput(int limit, int eachNumCnt, int iterCnt) {
        this.limit = limit;
        this.eachNumCnt = eachNumCnt;
        this.iterCnt = iterCnt;
    }

    /** {@inheritDoc} */
    @Override public IgniteSupplier<Stream<GroupTrainerCacheKey<Double>>> initialKeys(UUID trainingUUID) {
        int lim = limit;
        return () -> IntStream.range(0, lim).mapToObj(i -> new GroupTrainerCacheKey<>(i, 0.0, trainingUUID));
    }

    /** */
    int limit() {
        return limit;
    }

    /** */
    int iterCnt() {
        return iterCnt;
    }

    /** */
    int eachNumberCount() {
        return eachNumCnt;
    }
}
