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

package org.apache.ignite.examples.ml.dlearn;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionStorage;
import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnContext;
import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnContextFactory;
import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnPartition;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformers;
import org.apache.ignite.ml.dlearn.dataset.AbstractDLearnContextWrapper;
import org.apache.ignite.ml.dlearn.dataset.DLearnLabeledDataset;
import org.apache.ignite.ml.dlearn.dataset.part.DLearnLabeledDatasetPartition;

/**
 * How to transform dataset into algorithm-specific view?
 */
public class TransformerExample {
    /** Run example. */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> D-Learn Dataset example started.");

            IgniteCache<Integer, Person> persons = createCache(ignite);

            // Initialization of the d-learn context. After this step context cache will be created with partitions
            // placed on the same nodes as the upstream Ignite Cache.
            CacheDLearnContext<CacheDLearnPartition<Integer, Person>> ctx =
                new CacheDLearnContextFactory<>(ignite, persons).createContext();

            // Loading of the d-learn context. During this step data will be transferred from the upstream cache to
            // context cache with specified transformation (it will be performed locally because partitions are on the
            // same nodes). In this case for every partition in upstream cache will be created labeled dataset partition
            // and this new partition will be filled with help of specified feature and label extractors.
            DLearnLabeledDataset<Double> dataset = ctx.transform(DLearnContextTransformers.cacheToLabeledDataset(
                (k, v) -> new double[]{ v.age },
                (k, v) -> v.salary
            ));

            AlgorithmSpecificContext algorithmSpecificCtx = dataset.transform(new AlgorithmSpecificTransformer());
            System.out.println("Result: " + algorithmSpecificCtx.solve());
        }
    }

    /**
     * Algorithm-specific partition.
     */
    private static class AlgorithmSpecificPartition implements AutoCloseable {
        /** */
        private static final String MATRIX_A_KEY = "matrix_a";

        /** */
        private static final String VECTOR_B_KEY = "vector_b";

        /** */
        private static final String ROWS_KEY = "rows";

        /** */
        private final DLearnPartitionStorage storage;

        /** */
        public AlgorithmSpecificPartition(DLearnPartitionStorage storage) {
            this.storage = storage;
        }

        /** */
        public double[] getMatrixA() {
            return storage.get(MATRIX_A_KEY);
        }

        /** */
        public void setMatrixA(double[] matrixA) {
            storage.put(MATRIX_A_KEY, matrixA);
        }

        /** */
        public Integer getRows() {
            return storage.get(ROWS_KEY);
        }

        /** */
        public void setRows(int rows) {
            storage.put(ROWS_KEY, rows);
        }

        /** */
        public double[] getVectorB() {
            return storage.get(VECTOR_B_KEY);
        }

        /** */
        public void setVectorB(double[] vectorB) {
            storage.put(VECTOR_B_KEY, vectorB);
        }

        /** */
        @Override public void close() {
            storage.remove(MATRIX_A_KEY);
            storage.remove(VECTOR_B_KEY);
            storage.remove(ROWS_KEY);
        }
    }

    /**
     * Algorithm-specific representation of the context.
     */
    private static class AlgorithmSpecificContext extends AbstractDLearnContextWrapper<AlgorithmSpecificPartition> {
        /** */
        public AlgorithmSpecificContext(
            DLearnContext<AlgorithmSpecificPartition> delegate) {
            super(delegate);
        }

        /** */
        public double solve() {
            return compute(part -> {
                double[] matrixA = part.getMatrixA();
                double[] vectorB = part.getVectorB();
                Integer rows = part.getRows();
                // do something to solve...
                return 42.0;
            }, (a, b) -> a == null ? b : a + b);
        }
    }

    /**
     * Algorithm-specific transformer.
     */
    private static class AlgorithmSpecificTransformer implements DLearnContextTransformer<DLearnLabeledDatasetPartition<Double>,
        AlgorithmSpecificPartition, AlgorithmSpecificContext> {
        /** */
        private static final long serialVersionUID = 2109144841306143061L;

        /** */
        @Override public void transform(DLearnLabeledDatasetPartition<Double> oldPart,
            AlgorithmSpecificPartition newPart) {
            double[] features = oldPart.getFeatures();

            if (features != null) {
                Object[] labels = oldPart.getLabels();
                int rows = oldPart.getRows();

                double[] aMatrix = new double[features.length + rows];
                double[] bVector = new double[rows];

                for (int i = 0; i < rows; i++) {
                    aMatrix[i] = 1.0;
                    bVector[i] = (Double) labels[i];
                }

                newPart.setMatrixA(aMatrix);
                newPart.setVectorB(bVector);
                newPart.setRows(rows);
            }
        }

        /** */
        @Override public AlgorithmSpecificContext wrapContext(
            DLearnContext<AlgorithmSpecificPartition> ctx) {
            return new AlgorithmSpecificContext(ctx);
        }

        /** */
        @Override public AlgorithmSpecificPartition createPartition(DLearnPartitionStorage storage) {
            return new AlgorithmSpecificPartition(storage);
        }
    }

    /** */
    private static IgniteCache<Integer, Person> createCache(Ignite ignite) {
        IgniteCache<Integer, Person> persons = ignite.createCache("PERSONS");
        persons.put(1, new Person("Mike", 42, 10000));
        persons.put(2, new Person("John", 32, 64000));
        persons.put(3, new Person("George", 53, 120000));
        persons.put(4, new Person("Karl", 24, 70000));
        return persons;
    }

    /** */
    private static class Person {
        /** */
        private final String name;

        /** */
        private final double age;

        /** */
        private final double salary;

        /** */
        public Person(String name, double age, double salary) {
            this.name = name;
            this.age = age;
            this.salary = salary;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public double getAge() {
            return age;
        }

        /** */
        public double getSalary() {
            return salary;
        }
    }
}
