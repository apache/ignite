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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnContext;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnContextFactory;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnPartition;
import org.apache.ignite.ml.dlearn.context.transformer.DLearnContextTransformers;
import org.apache.ignite.ml.dlearn.dataset.DLearnDataset;

/**
 * How to create a d-learn dataset from an existing local data?
 */
public class LocalDatasetExample {
    /** Run example. */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> D-Learn Local Dataset example started.");

            Map<Integer, Person> persons = createCache(ignite);

            // Initialization of the d-learn context. This context uses local hash map as an underlying storage.
            LocalDLearnContext<LocalDLearnPartition<Integer, Person>> ctx =
                new LocalDLearnContextFactory<>(persons, 2).createContext();

            // Loading of the d-learn context. During this step data will be transferred from the generic local
            // partition to dataset partition.
            DLearnDataset<?> dataset = ctx.transform(DLearnContextTransformers.localToDataset((k, v) -> {
                double[] row = new double[2];
                row[0] = v.age;
                row[1] = v.salary;
                return row;
            }));

            // Calculation of the mean value. This calculation will be performed in map-reduce manner.
            double[] mean = dataset.mean(new int[] {0, 1});
            System.out.println("Mean \n\t" + Arrays.toString(mean));

            // Calculation of the standard deviation. This calculation will be performed in map-reduce manner.
            double[] std = dataset.std(new int[] {0, 1});
            System.out.println("Standard deviation \n\t" + Arrays.toString(std));

            // Calculation of the covariance matrix.  This calculation will be performed in map-reduce manner.
            double[][] cov = dataset.cov(new int[] {0, 1});
            System.out.println("Covariance matrix ");
            for (double[] row : cov)
                System.out.println("\t" + Arrays.toString(row));

            // Calculation of the correlation matrix.  This calculation will be performed in map-reduce manner.
            double[][] corr = dataset.corr(new int[] {0, 1});
            System.out.println("Correlation matrix ");
            for (double[] row : corr)
                System.out.println("\t" + Arrays.toString(row));

            System.out.println(">>> D-Learn Local Dataset example completed.");
        }
    }

    /** */
    private static Map<Integer, Person> createCache(Ignite ignite) {
        Map<Integer, Person> persons = new HashMap<>();
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
