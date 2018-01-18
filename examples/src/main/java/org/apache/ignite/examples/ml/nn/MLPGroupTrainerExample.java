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

package org.apache.ignite.examples.ml.nn;

import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.Tracer;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.LabeledVectorsCache;
import org.apache.ignite.ml.nn.MLPGroupUpdateTrainerCacheInput;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.nn.initializers.RandomInitializer;
import org.apache.ignite.ml.nn.trainers.distributed.MLPGroupUpdateTrainer;
import org.apache.ignite.ml.optimization.updatecalculators.RPropParameterUpdate;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.thread.IgniteThread;

/**
 * Example of using distributed {@link MultilayerPerceptron}.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p>
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class MLPGroupTrainerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        // IMPL NOTE based on MLPGroupTrainerTest#testXOR
        System.out.println(">>> Distributed multilayer perceptron example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
            // because we create ignite cache internally.
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                MLPGroupTrainerExample.class.getSimpleName(), () -> {

                int samplesCnt = 10000;

                Matrix xorInputs = new DenseLocalOnHeapMatrix(
                    new double[][] {{0.0, 0.0}, {0.0, 1.0}, {1.0, 0.0}, {1.0, 1.0}},
                    StorageConstants.ROW_STORAGE_MODE).transpose();

                Matrix xorOutputs = new DenseLocalOnHeapMatrix(
                    new double[][] {{0.0}, {1.0}, {1.0}, {0.0}},
                    StorageConstants.ROW_STORAGE_MODE).transpose();

                MLPArchitecture conf = new MLPArchitecture(2).
                    withAddedLayer(10, true, Activators.RELU).
                    withAddedLayer(1, false, Activators.SIGMOID);

                IgniteCache<Integer, LabeledVector<Vector, Vector>> cache = LabeledVectorsCache.createNew(ignite);
                String cacheName = cache.getName();
                Random rnd = new Random(12345L);

                try (IgniteDataStreamer<Integer, LabeledVector<Vector, Vector>> streamer =
                         ignite.dataStreamer(cacheName)) {
                    streamer.perNodeBufferSize(100);

                    for (int i = 0; i < samplesCnt; i++) {
                        int col = Math.abs(rnd.nextInt()) % 4;
                        streamer.addData(i, new LabeledVector<>(xorInputs.getCol(col), xorOutputs.getCol(col)));
                    }
                }

                int totalCnt = 100;
                int failCnt = 0;
                MLPGroupUpdateTrainer<RPropParameterUpdate> trainer = MLPGroupUpdateTrainer.getDefault(ignite).
                    withSyncPeriod(3).
                    withTolerance(0.001).
                    withMaxGlobalSteps(20);

                for (int i = 0; i < totalCnt; i++) {

                    MLPGroupUpdateTrainerCacheInput trainerInput = new MLPGroupUpdateTrainerCacheInput(conf,
                        new RandomInitializer(rnd), 6, cache, 10);

                    MultilayerPerceptron mlp = trainer.train(trainerInput);

                    Matrix predict = mlp.apply(xorInputs);

                    System.out.println(">>> Prediction data at step " + i + " of total " + totalCnt + ":");

                    Tracer.showAscii(predict);

                    System.out.println("Difference estimate: " + xorOutputs.getRow(0).minus(predict.getRow(0)).kNorm(2));

                    failCnt += closeEnough(xorOutputs.getRow(0), predict.getRow(0)) ? 0 : 1;
                }

                double failRatio = (double)failCnt / totalCnt;

                System.out.println("\n>>> Fail percentage: " + (failRatio * 100) + "%.");

                System.out.println("\n>>> Distributed multilayer perceptron example completed.");
            });

            igniteThread.start();

            igniteThread.join();
        }
    }

    /** */
    private static boolean closeEnough(Vector v1, Vector v2) {
        return v1.minus(v2).kNorm(2) < 5E-1;
    }
}
