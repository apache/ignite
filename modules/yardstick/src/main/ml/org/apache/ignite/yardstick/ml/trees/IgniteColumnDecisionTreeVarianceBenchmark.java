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

package org.apache.ignite.yardstick.ml.trees;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.trees.trainers.columnbased.contsplitcalcs.ContinuousSplitCalculators;
import org.apache.ignite.ml.trees.trainers.columnbased.regcalcs.RegionCalculators;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
@SuppressWarnings("unused")
public class IgniteColumnDecisionTreeVarianceBenchmark extends IgniteAbstractBenchmark {
    /** */
    @IgniteInstanceResource
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
        // because we create ignite cache internally.
        IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
            this.getClass().getSimpleName(), new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                // IMPL NOTE originally taken from ColumnDecisionTreeTrainerTest#testCacheMixed
                int totalPts = 1 << 10;
                int featCnt = 2;

                HashMap<Integer, Integer> catsInfo = new HashMap<>();
                catsInfo.put(1, 3);

                SplitDataGenerator<DenseLocalOnHeapVector> gen
                    = new SplitDataGenerator<>(
                    featCnt, catsInfo, () -> new DenseLocalOnHeapVector(featCnt + 1)).
                    split(0, 1, new int[] {0, 2}).
                    split(1, 0, -10.0);

                gen.testByGen(totalPts,
                    ContinuousSplitCalculators.VARIANCE, RegionCalculators.VARIANCE, RegionCalculators.MEAN, ignite);
            }
        });

        igniteThread.start();

        igniteThread.join();

        return true;
    }
}
