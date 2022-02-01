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

package org.apache.ignite.examples.ml.dataset;

import java.io.Serializable;
import java.util.Arrays;
import com.github.fommil.netlib.BLAS;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.primitive.DatasetWrapper;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.developer.PatchedPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Example that shows how to implement your own algorithm (<a href="https://en.wikipedia.org/wiki/Gradient_descent">gradient</a>
 * descent trainer for linear regression) which uses dataset as an underlying infrastructure.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with simple test data.</p>
 * <p>
 * After that it creates an algorithm specific dataset to perform linear regression as described in more detail
 * below.</p>
 * <p>
 * Finally, this example trains linear regression model using gradient descent and outputs the result.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this functionality further.</p>
 * <p>
 * The common idea behind using algorithm specific datasets is to write a simple local version algorithm at first, then
 * find operations which involve data manipulations, and finally define algorithm specific version of the dataset
 * extended by introducing these new operations. As a result your algorithm will work with extended dataset (based on
 * {@link DatasetWrapper}) in a sequential manner.</p>
 * <p>
 * In this example we need to implement gradient descent. This is iterative method that involves calculation of gradient
 * on every step. In according with the common idea we define {@link AlgorithmSpecificDatasetExample.AlgorithmSpecificDataset}
 * - extended version of {@code Dataset} with {@code gradient} method. As a result our gradient descent method looks
 * like a simple loop where every iteration includes call of the {@code gradient} method. In the example we want to keep
 * iteration number as well for logging. Iteration number cannot be recovered from the {@code upstream} data and we need
 * to keep it in the custom partition {@code context} which is represented by {@link
 * AlgorithmSpecificDatasetExample.AlgorithmSpecificPartitionContext} class.</p>
 */
public class AlgorithmSpecificDatasetExample {
    /**
     * Run example.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Algorithm Specific Dataset example started.");

            IgniteCache<Integer, Vector> persons = null;
            try {
                persons = createCache(ignite);

                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(1);

                IgniteFunction<LabeledVector<Double>, LabeledVector<double[]>> func =
                    lv -> new LabeledVector<>(lv.features(), new double[] {lv.label()});

                //NOTE: This class is part of Developer API and all lambdas should be loaded on server manually.
                Preprocessor<Integer, Vector> preprocessor = new PatchedPreprocessor<>(func, vectorizer);
                // Creates a algorithm specific dataset to perform linear regression. Here we define the way features and
                // labels are extracted, and partition data and context are created.
                SimpleLabeledDatasetDataBuilder<Integer, Vector, AlgorithmSpecificPartitionContext> builder =
                    new SimpleLabeledDatasetDataBuilder<>(preprocessor);

                IgniteBiFunction<SimpleLabeledDatasetData, AlgorithmSpecificPartitionContext, SimpleLabeledDatasetData> builderFun =
                    (data, ctx) -> {
                        double[] features = data.getFeatures();
                        int rows = data.getRows();

                        // Makes a copy of features to supplement it by columns with values equal to 1.0.
                        double[] a = new double[features.length + rows];
                        Arrays.fill(a, 1.0);

                        System.arraycopy(features, 0, a, rows, features.length);

                        return new SimpleLabeledDatasetData(a, data.getLabels(), rows);
                    };

                try (AlgorithmSpecificDataset dataset = DatasetFactory.create(
                    ignite,
                    persons,
                    (env, upstream, upstreamSize) -> new AlgorithmSpecificPartitionContext(),
                    builder.andThen(builderFun)
                ).wrap(AlgorithmSpecificDataset::new)) {
                    // Trains linear regression model using gradient descent.
                    double[] linearRegressionMdl = new double[2];

                    for (int i = 0; i < 1000; i++) {
                        double[] gradient = dataset.gradient(linearRegressionMdl);

                        if (BLAS.getInstance().dnrm2(gradient.length, gradient, 1) < 1e-4)
                            break;

                        for (int j = 0; j < gradient.length; j++)
                            linearRegressionMdl[j] -= 0.1 / persons.size() * gradient[j];
                    }

                    System.out.println("Linear Regression Model: " + Arrays.toString(linearRegressionMdl));
                }

                System.out.println(">>> Algorithm Specific Dataset example completed.");
            }
            finally {
                persons.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }

    /**
     * Algorithm specific dataset. Extended version of {@code Dataset} with {@code gradient} method.
     */
    private static class AlgorithmSpecificDataset
        extends DatasetWrapper<AlgorithmSpecificPartitionContext, SimpleLabeledDatasetData> {
        /**
         * BLAS (Basic Linear Algebra Subprograms) instance.
         */
        private static final BLAS blas = BLAS.getInstance();

        /**
         * Constructs a new instance of dataset wrapper that delegates {@code compute} actions to the actual delegate.
         *
         * @param delegate Delegate that performs {@code compute} actions.
         */
        AlgorithmSpecificDataset(
            Dataset<AlgorithmSpecificPartitionContext, SimpleLabeledDatasetData> delegate) {
            super(delegate);
        }

        /**
         * Calculate gradient.
         */
        double[] gradient(double[] x) {
            return computeWithCtx((ctx, data, partIdx) -> {
                double[] tmp = Arrays.copyOf(data.getLabels(), data.getRows());
                int featureCols = data.getFeatures().length / data.getRows();
                blas.dgemv("N", data.getRows(), featureCols, 1.0, data.getFeatures(),
                    Math.max(1, data.getRows()), x, 1, -1.0, tmp, 1);

                double[] res = new double[featureCols];
                blas.dgemv("T", data.getRows(), featureCols, 1.0, data.getFeatures(),
                    Math.max(1, data.getRows()), tmp, 1, 0.0, res, 1);

                int iteration = ctx.getIteration();

                System.out.println("Iteration " + iteration + " on partition " + partIdx
                    + " completed with local result " + Arrays.toString(res));

                ctx.setIteration(iteration + 1);

                return res;
            }, this::sum);
        }

        /**
         * Sum of two vectors.
         */
        public double[] sum(double[] a, double[] b) {
            if (a == null)
                return b;

            if (b == null)
                return a;

            blas.daxpy(a.length, 1.0, a, 1, b, 1);

            return b;
        }
    }

    /**
     * Algorithm specific partition context which keeps iteration number.
     */
    private static class AlgorithmSpecificPartitionContext implements Serializable {
        /**
         *
         */
        private static final long serialVersionUID = 1887368924266684044L;

        /**
         * Iteration number.
         */
        private int iteration;

        /**
         *
         */
        int getIteration() {
            return iteration;
        }

        /**
         *
         */
        void setIteration(int iteration) {
            this.iteration = iteration;
        }
    }

    /**
     *
     */
    private static IgniteCache<Integer, Vector> createCache(Ignite ignite) {
        CacheConfiguration<Integer, Vector> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName("PERSONS");
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 2));

        IgniteCache<Integer, Vector> persons = ignite.createCache(cacheConfiguration);

        persons.put(1, new DenseVector(new Serializable[] {"Mike", 42, 10000}));
        persons.put(2, new DenseVector(new Serializable[] {"John", 32, 64000}));
        persons.put(3, new DenseVector(new Serializable[] {"George", 53, 120000}));
        persons.put(4, new DenseVector(new Serializable[] {"Karl", 24, 70000}));

        return persons;
    }
}
