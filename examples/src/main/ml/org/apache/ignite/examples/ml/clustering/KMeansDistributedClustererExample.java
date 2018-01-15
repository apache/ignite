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

package org.apache.ignite.examples.ml.clustering;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.ml.math.matrix.SparseDistributedMatrixExample;
import org.apache.ignite.ml.clustering.KMeansDistributedClusterer;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.impls.matrix.SparseDistributedMatrix;
import org.apache.ignite.thread.IgniteThread;

/**
 * <p>
 * Example of using {@link KMeansDistributedClusterer}.</p>
 * <p>
 * Note that in this example we cannot guarantee order in which nodes return results of intermediate
 * computations and therefore algorithm can return different results.</p>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p>
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class KMeansDistributedClustererExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        // IMPL NOTE based on KMeansDistributedClustererTestSingleNode#testClusterizationOnDatasetWithObviousStructure
        System.out.println(">>> K-means distributed clusterer example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
            // because we create ignite cache internally.
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                SparseDistributedMatrixExample.class.getSimpleName(), () -> {

                int ptsCnt = 10000;

                SparseDistributedMatrix points = new SparseDistributedMatrix(ptsCnt, 2,
                    StorageConstants.ROW_STORAGE_MODE, StorageConstants.RANDOM_ACCESS_MODE);

                DatasetWithObviousStructure dataset = new DatasetWithObviousStructure(10000);

                List<Vector> massCenters = dataset.generate(points);

                EuclideanDistance dist = new EuclideanDistance();

                KMeansDistributedClusterer clusterer = new KMeansDistributedClusterer(dist, 3, 100, 1L);

                Vector[] resCenters = clusterer.cluster(points, 4).centers();

                System.out.println("Mass centers:");
                massCenters.forEach(Tracer::showAscii);

                System.out.println("Cluster centers:");
                Arrays.asList(resCenters).forEach(Tracer::showAscii);

                points.destroy();

                System.out.println("\n>>> K-means distributed clusterer example completed.");
            });

            igniteThread.start();

            igniteThread.join();
        }
    }
}
