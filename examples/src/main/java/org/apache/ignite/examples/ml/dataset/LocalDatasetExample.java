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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.dataset.model.Person;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.primitive.SimpleDataset;

/**
 * Example that shows how to create dataset based on an existing local storage and then use it to calculate {@code mean}
 * and {@code std} values as well as {@code covariance} and {@code correlation} matrices.
 */
public class LocalDatasetExample {
    /** Run example. */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Local Dataset example started.");

            Map<Integer, Person> persons = createCache(ignite);

            // Creates a local simple dataset containing features and providing standard dataset API.
            try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(
                persons,
                2,
                (k, v) -> new double[]{ v.getAge(), v.getSalary() }
            )) {
                // Calculation of the mean value. This calculation will be performed in map-reduce manner.
                double[] mean = dataset.mean();
                System.out.println("Mean \n\t" + Arrays.toString(mean));

                // Calculation of the standard deviation. This calculation will be performed in map-reduce manner.
                double[] std = dataset.std();
                System.out.println("Standard deviation \n\t" + Arrays.toString(std));

                // Calculation of the covariance matrix.  This calculation will be performed in map-reduce manner.
                double[][] cov = dataset.cov();
                System.out.println("Covariance matrix ");
                for (double[] row : cov)
                    System.out.println("\t" + Arrays.toString(row));

                // Calculation of the correlation matrix.  This calculation will be performed in map-reduce manner.
                double[][] corr = dataset.corr();
                System.out.println("Correlation matrix ");
                for (double[] row : corr)
                    System.out.println("\t" + Arrays.toString(row));
            }

            System.out.println(">>> Local Dataset example completed.");
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
}
