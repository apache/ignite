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

import com.github.fommil.netlib.BLAS;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ml.dataset.model.Person;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.primitive.DatasetWrapper;
import org.apache.ignite.ml.dataset.primitive.builder.data.SimpleLabeledDatasetDataBuilder;
import org.apache.ignite.ml.dataset.primitive.data.SimpleLabeledDatasetData;

/**
 * Example that shows how to implement your own algorithm (gradient descent trainer for linear regression) which uses
 * dataset as an underlying infrastructure.
 *
 * The common idea behind using algorithm specific datasets is to write a simple local version algorithm at first, then
 * find operations which involves data manipulations, and finally define algorithm specific version of the dataset
 * extended by introducing these new operations. As result your algorithm will work with extended dataset (based on
 * {@link DatasetWrapper}) in a sequential manner.
 *
 * In this example we need to implement gradient descent. This is iterative method that involves calculation of gradient
 * on every step. In according with the common idea we defines
 * {@link AlgorithmSpecificDatasetExample.AlgorithmSpecificDataset} - extended version of {@code Dataset} with
 * {@code gradient} method. As result our gradient descent method looks like a simple loop where every iteration
 * includes call of the {@code gradient} method. In the example we want to keep iteration number as well for logging.
 * Iteration number cannot be recovered from the {@code upstream} data and we need to keep it in the custom
 * partition {@code context} which is represented by
 * {@link AlgorithmSpecificDatasetExample.AlgorithmSpecificPartitionContext} class.
 */
public class AlgorithmSpecificDatasetExample {
    /** Run example. */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Algorithm Specific Dataset example started.");

            IgniteCache<Integer, Person> persons = createCache(ignite);

            // Creates a algorithm specific dataset to perform linear regression. Here we defines the way features and
            // labels are extracted, and partition data and context are created.
            try (AlgorithmSpecificDataset dataset = DatasetFactory.create(
                ignite,
                persons,
                (upstream, upstreamSize) -> new AlgorithmSpecificPartitionContext(),
                new SimpleLabeledDatasetDataBuilder<Integer, Person, AlgorithmSpecificPartitionContext>(
                    (k, v) -> new double[] {v.getAge()},
                    (k, v) -> new double[] {v.getSalary()}
                ).andThen((data, ctx) -> {
                    double[] features = data.getFeatures();
                    int rows = data.getRows();

                    // Makes a copy of features to supplement it by columns with values equal to 1.0.
                    double[] a = new double[features.length + rows];

                    for (int i = 0; i < rows; i++)
                        a[i] = 1.0;

                    System.arraycopy(features, 0, a, rows, features.length);

                    return new SimpleLabeledDatasetData(a, data.getLabels(), rows);
                })
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
    }

    /**
     * Algorithm specific dataset. Extended version of {@code Dataset} with {@code gradient} method.
     */
    private static class AlgorithmSpecificDataset
        extends DatasetWrapper<AlgorithmSpecificPartitionContext, SimpleLabeledDatasetData> {
        /** BLAS (Basic Linear Algebra Subprograms) instance. */
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

        /** Calculate gradient. */
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

        /** Sum of two vectors. */
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
        /** */
        private static final long serialVersionUID = 1887368924266684044L;

        /** Iteration number. */
        private int iteration;

        /** */
        public int getIteration() {
            return iteration;
        }

        /** */
        public void setIteration(int iteration) {
            this.iteration = iteration;
        }
    }

    /** */
    private static IgniteCache<Integer, Person> createCache(Ignite ignite) {
        CacheConfiguration<Integer, Person> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName("PERSONS");
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 2));

        IgniteCache<Integer, Person> persons = ignite.createCache(cacheConfiguration);

        persons.put(1, new Person("Mike", 1, 1));
        persons.put(2, new Person("John", 2, 2));
        persons.put(3, new Person("George", 3, 3));
        persons.put(4, new Person("Karl", 4, 4));

        return persons;
    }
}
