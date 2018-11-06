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

package org.apache.ignite.examples.ml.svm.multiclass;

import java.util.Arrays;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.svm.SVMLinearMultiClassClassificationModel;
import org.apache.ignite.ml.svm.SVMLinearMultiClassClassificationTrainer;
import org.apache.ignite.thread.IgniteThread;

/**
 * Run SVM multi-class classification trainer over distributed dataset to build two models:
 * one with minmaxscaling and one without minmaxscaling.
 *
 * @see SVMLinearMultiClassClassificationModel
 */
public class SVMMultiClassClassificationExample {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> SVM Multi-class classification model over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                SVMMultiClassClassificationExample.class.getSimpleName(), () -> {
                IgniteCache<Integer, Vector> dataCache = getTestCache(ignite);

                SVMLinearMultiClassClassificationTrainer trainer = new SVMLinearMultiClassClassificationTrainer();

                SVMLinearMultiClassClassificationModel mdl = trainer.fit(
                    ignite,
                    dataCache,
                    (k, v) -> {
                        double[] arr = v.asArray();
                        return VectorUtils.of(Arrays.copyOfRange(arr, 1, arr.length));
                    },
                    (k, v) -> v.get(0)
                );

                System.out.println(">>> SVM Multi-class model");
                System.out.println(mdl.toString());

                MinMaxScalerTrainer<Integer, Vector> normalizationTrainer = new MinMaxScalerTrainer<>();

                IgniteBiFunction<Integer, Vector, Vector> preprocessor = normalizationTrainer.fit(
                    ignite,
                    dataCache,
                    (k, v) -> {
                        double[] arr = v.asArray();
                        return VectorUtils.of(Arrays.copyOfRange(arr, 1, arr.length));
                    }
                );

                SVMLinearMultiClassClassificationModel mdlWithNormalization = trainer.fit(
                    ignite,
                    dataCache,
                    preprocessor,
                    (k, v) -> v.get(0)
                );

                System.out.println(">>> SVM Multi-class model with minmaxscaling");
                System.out.println(mdlWithNormalization.toString());

                System.out.println(">>> ----------------------------------------------------------------");
                System.out.println(">>> | Prediction\t| Prediction with Normalization\t| Ground Truth\t|");
                System.out.println(">>> ----------------------------------------------------------------");

                int amountOfErrors = 0;
                int amountOfErrorsWithNormalization = 0;
                int totalAmount = 0;

                // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
                int[][] confusionMtx = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
                int[][] confusionMtxWithNormalization = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};

                try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, Vector> observation : observations) {
                        double[] val = observation.getValue().asArray();
                        double[] inputs = Arrays.copyOfRange(val, 1, val.length);
                        double groundTruth = val[0];

                        double prediction = mdl.apply(new DenseVector(inputs));
                        double predictionWithNormalization = mdlWithNormalization.apply(new DenseVector(inputs));

                        totalAmount++;

                        // Collect data for model
                        if(groundTruth != prediction)
                            amountOfErrors++;

                        int idx1 = (int)prediction == 1 ? 0 : ((int)prediction == 3 ? 1 : 2);
                        int idx2 = (int)groundTruth == 1 ? 0 : ((int)groundTruth == 3 ? 1 : 2);

                        confusionMtx[idx1][idx2]++;

                        // Collect data for model with minmaxscaling
                        if(groundTruth != predictionWithNormalization)
                            amountOfErrorsWithNormalization++;

                        idx1 = (int)predictionWithNormalization == 1 ? 0 : ((int)predictionWithNormalization == 3 ? 1 : 2);
                        idx2 = (int)groundTruth == 1 ? 0 : ((int)groundTruth == 3 ? 1 : 2);

                        confusionMtxWithNormalization[idx1][idx2]++;

                        System.out.printf(">>> | %.4f\t\t| %.4f\t\t\t\t\t\t| %.4f\t\t|\n", prediction, predictionWithNormalization, groundTruth);
                    }
                    System.out.println(">>> ----------------------------------------------------------------");
                    System.out.println("\n>>> -----------------SVM model-------------");
                    System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                    System.out.println("\n>>> Accuracy " + (1 - amountOfErrors / (double)totalAmount));
                    System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));

                    System.out.println("\n>>> -----------------SVM model with Normalization-------------");
                    System.out.println("\n>>> Absolute amount of errors " + amountOfErrorsWithNormalization);
                    System.out.println("\n>>> Accuracy " + (1 - amountOfErrorsWithNormalization / (double)totalAmount));
                    System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtxWithNormalization));
                }
            });

            igniteThread.start();
            igniteThread.join();
        }
    }

    /**
     * Fills cache with data and returns it.
     *
     * @param ignite Ignite instance.
     * @return Filled Ignite Cache.
     */
    private static IgniteCache<Integer, Vector> getTestCache(Ignite ignite) {
        CacheConfiguration<Integer, Vector> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TEST_" + UUID.randomUUID());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));

        IgniteCache<Integer, Vector> cache = ignite.createCache(cacheConfiguration);

        for (int i = 0; i < data.length; i++)
            cache.put(i, VectorUtils.of(data[i]));

        return cache;
    }

    /** The preprocessed Glass dataset from the Machine Learning Repository https://archive.ics.uci.edu/ml/datasets/Glass+Identification
     *  There are 3 classes with labels: 1 {building_windows_float_processed}, 3 {vehicle_windows_float_processed}, 7 {headlamps}.
     *  Feature names: 'Na-Sodium', 'Mg-Magnesium', 'Al-Aluminum', 'Ba-Barium', 'Fe-Iron'.
     */
    private static final double[][] data = {
        {1, 1.52101, 4.49, 1.10, 0.00, 0.00},
        {1, 1.51761, 3.60, 1.36, 0.00, 0.00},
        {1, 1.51618, 3.55, 1.54, 0.00, 0.00},
        {1, 1.51766, 3.69, 1.29, 0.00, 0.00},
        {1, 1.51742, 3.62, 1.24, 0.00, 0.00},
        {1, 1.51596, 3.61, 1.62, 0.00, 0.26},
        {1, 1.51743, 3.60, 1.14, 0.00, 0.00},
        {1, 1.51756, 3.61, 1.05, 0.00, 0.00},
        {1, 1.51918, 3.58, 1.37, 0.00, 0.00},
        {1, 1.51755, 3.60, 1.36, 0.00, 0.11},
        {1, 1.51571, 3.46, 1.56, 0.00, 0.24},
        {1, 1.51763, 3.66, 1.27, 0.00, 0.00},
        {1, 1.51589, 3.43, 1.40, 0.00, 0.24},
        {1, 1.51748, 3.56, 1.27, 0.00, 0.17},
        {1, 1.51763, 3.59, 1.31, 0.00, 0.00},
        {1, 1.51761, 3.54, 1.23, 0.00, 0.00},
        {1, 1.51784, 3.67, 1.16, 0.00, 0.00},
        {1, 1.52196, 3.85, 0.89, 0.00, 0.00},
        {1, 1.51911, 3.73, 1.18, 0.00, 0.00},
        {1, 1.51735, 3.54, 1.69, 0.00, 0.07},
        {1, 1.51750, 3.55, 1.49, 0.00, 0.19},
        {1, 1.51966, 3.75, 0.29, 0.00, 0.00},
        {1, 1.51736, 3.62, 1.29, 0.00, 0.00},
        {1, 1.51751, 3.57, 1.35, 0.00, 0.00},
        {1, 1.51720, 3.50, 1.15, 0.00, 0.00},
        {1, 1.51764, 3.54, 1.21, 0.00, 0.00},
        {1, 1.51793, 3.48, 1.41, 0.00, 0.00},
        {1, 1.51721, 3.48, 1.33, 0.00, 0.00},
        {1, 1.51768, 3.52, 1.43, 0.00, 0.00},
        {1, 1.51784, 3.49, 1.28, 0.00, 0.00},
        {1, 1.51768, 3.56, 1.30, 0.00, 0.14},
        {1, 1.51747, 3.50, 1.14, 0.00, 0.00},
        {1, 1.51775, 3.48, 1.23, 0.09, 0.22},
        {1, 1.51753, 3.47, 1.38, 0.00, 0.06},
        {1, 1.51783, 3.54, 1.34, 0.00, 0.00},
        {1, 1.51567, 3.45, 1.21, 0.00, 0.00},
        {1, 1.51909, 3.53, 1.32, 0.11, 0.00},
        {1, 1.51797, 3.48, 1.35, 0.00, 0.00},
        {1, 1.52213, 3.82, 0.47, 0.00, 0.00},
        {1, 1.52213, 3.82, 0.47, 0.00, 0.00},
        {1, 1.51793, 3.50, 1.12, 0.00, 0.00},
        {1, 1.51755, 3.42, 1.20, 0.00, 0.00},
        {1, 1.51779, 3.39, 1.33, 0.00, 0.00},
        {1, 1.52210, 3.84, 0.72, 0.00, 0.00},
        {1, 1.51786, 3.43, 1.19, 0.00, 0.30},
        {1, 1.51900, 3.48, 1.35, 0.00, 0.00},
        {1, 1.51869, 3.37, 1.18, 0.00, 0.16},
        {1, 1.52667, 3.70, 0.71, 0.00, 0.10},
        {1, 1.52223, 3.77, 0.79, 0.00, 0.00},
        {1, 1.51898, 3.35, 1.23, 0.00, 0.00},
        {1, 1.52320, 3.72, 0.51, 0.00, 0.16},
        {1, 1.51926, 3.33, 1.28, 0.00, 0.11},
        {1, 1.51808, 2.87, 1.19, 0.00, 0.00},
        {1, 1.51837, 2.84, 1.28, 0.00, 0.00},
        {1, 1.51778, 2.81, 1.29, 0.00, 0.09},
        {1, 1.51769, 2.71, 1.29, 0.00, 0.24},
        {1, 1.51215, 3.47, 1.12, 0.00, 0.31},
        {1, 1.51824, 3.48, 1.29, 0.00, 0.00},
        {1, 1.51754, 3.74, 1.17, 0.00, 0.00},
        {1, 1.51754, 3.66, 1.19, 0.00, 0.11},
        {1, 1.51905, 3.62, 1.11, 0.00, 0.00},
        {1, 1.51977, 3.58, 1.32, 0.69, 0.00},
        {1, 1.52172, 3.86, 0.88, 0.00, 0.11},
        {1, 1.52227, 3.81, 0.78, 0.00, 0.00},
        {1, 1.52172, 3.74, 0.90, 0.00, 0.07},
        {1, 1.52099, 3.59, 1.12, 0.00, 0.00},
        {1, 1.52152, 3.65, 0.87, 0.00, 0.17},
        {1, 1.52152, 3.65, 0.87, 0.00, 0.17},
        {1, 1.52152, 3.58, 0.90, 0.00, 0.16},
        {1, 1.52300, 3.58, 0.82, 0.00, 0.03},
        {3, 1.51769, 3.66, 1.11, 0.00, 0.00},
        {3, 1.51610, 3.53, 1.34, 0.00, 0.00},
        {3, 1.51670, 3.57, 1.38, 0.00, 0.10},
        {3, 1.51643, 3.52, 1.35, 0.00, 0.00},
        {3, 1.51665, 3.45, 1.76, 0.00, 0.17},
        {3, 1.52127, 3.90, 0.83, 0.00, 0.00},
        {3, 1.51779, 3.65, 0.65, 0.00, 0.00},
        {3, 1.51610, 3.40, 1.22, 0.00, 0.00},
        {3, 1.51694, 3.58, 1.31, 0.00, 0.00},
        {3, 1.51646, 3.40, 1.26, 0.00, 0.00},
        {3, 1.51655, 3.39, 1.28, 0.00, 0.00},
        {3, 1.52121, 3.76, 0.58, 0.00, 0.00},
        {3, 1.51776, 3.41, 1.52, 0.00, 0.00},
        {3, 1.51796, 3.36, 1.63, 0.00, 0.09},
        {3, 1.51832, 3.34, 1.54, 0.00, 0.00},
        {3, 1.51934, 3.54, 0.75, 0.15, 0.24},
        {3, 1.52211, 3.78, 0.91, 0.00, 0.37},
        {7, 1.51131, 3.20, 1.81, 1.19, 0.00},
        {7, 1.51838, 3.26, 2.22, 1.63, 0.00},
        {7, 1.52315, 3.34, 1.23, 0.00, 0.00},
        {7, 1.52247, 2.20, 2.06, 0.00, 0.00},
        {7, 1.52365, 1.83, 1.31, 1.68, 0.00},
        {7, 1.51613, 1.78, 1.79, 0.76, 0.00},
        {7, 1.51602, 0.00, 2.38, 0.64, 0.09},
        {7, 1.51623, 0.00, 2.79, 0.40, 0.09},
        {7, 1.51719, 0.00, 2.00, 1.59, 0.08},
        {7, 1.51683, 0.00, 1.98, 1.57, 0.07},
        {7, 1.51545, 0.00, 2.68, 0.61, 0.05},
        {7, 1.51556, 0.00, 2.54, 0.81, 0.01},
        {7, 1.51727, 0.00, 2.34, 0.66, 0.00},
        {7, 1.51531, 0.00, 2.66, 0.64, 0.00},
        {7, 1.51609, 0.00, 2.51, 0.53, 0.00},
        {7, 1.51508, 0.00, 2.25, 0.63, 0.00},
        {7, 1.51653, 0.00, 1.19, 0.00, 0.00},
        {7, 1.51514, 0.00, 2.42, 0.56, 0.00},
        {7, 1.51658, 0.00, 1.99, 1.71, 0.00},
        {7, 1.51617, 0.00, 2.27, 0.67, 0.00},
        {7, 1.51732, 0.00, 1.80, 1.55, 0.00},
        {7, 1.51645, 0.00, 1.87, 1.38, 0.00},
        {7, 1.51831, 0.00, 1.82, 2.88, 0.00},
        {7, 1.51640, 0.00, 2.74, 0.54, 0.00},
        {7, 1.51623, 0.00, 2.88, 1.06, 0.00},
        {7, 1.51685, 0.00, 1.99, 1.59, 0.00},
        {7, 1.52065, 0.00, 2.02, 1.64, 0.00},
        {7, 1.51651, 0.00, 1.94, 1.57, 0.00},
        {7, 1.51711, 0.00, 2.08, 1.67, 0.00},
    };
}
