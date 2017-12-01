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

package org.apache.ignite.yardstick.ml;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.ml.clustering.KMeansDistributedClusterer;
import org.apache.ignite.ml.math.EuclideanDistance;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.thread.IgniteThread;
import org.apache.ignite.yardstick.IgniteAbstractBenchmark;
import org.yardstickframework.BenchmarkUtils;

/**
 * Ignite benchmark that performs ML Grid operations.
 */
@SuppressWarnings("unused")
public class IgniteKMeansDistributedClustererBenchmark extends IgniteAbstractBenchmark {
    /** */
    private static AtomicBoolean startLogged = new AtomicBoolean(false);

    /** */
    @IgniteInstanceResource
    Ignite ignite;

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        if (!startLogged.getAndSet(true))
            BenchmarkUtils.println("Starting " + this.getClass().getSimpleName());

        final DataChanger.Scale scale = new DataChanger.Scale();

        // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
        // because we create ignite cache internally.
        IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
            this.getClass().getSimpleName(), new Runnable() {
            /** {@inheritDoc} */
            @Override public void run() {
                // IMPL NOTE originally taken from KMeansDistributedClustererTest
                KMeansDistributedClusterer clusterer = new KMeansDistributedClusterer(
                    new EuclideanDistance(), 1, 1, 1L);

                double[] v1 = scale.mutate(new double[] {1959, 325100});
                double[] v2 = scale.mutate(new double[] {1960, 373200});

                SparseDistributedMatrix points = new SparseDistributedMatrix(
                    2, 2, StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

                points.setRow(0, v1);
                points.setRow(1, v2);

                clusterer.cluster(points, 1);

                points.destroy();
            }
        });

        igniteThread.start();

        igniteThread.join();

        return false;
    }
}
