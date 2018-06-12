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

package org.apache.ignite.examples.ml.preprocessing;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ml.dataset.model.Person;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.primitive.SimpleDataset;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.preprocessing.binarization.BinarizationTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;

/**
 * Example that shows how to use normalization preprocessor to normalize each vector in the given data.
 */
public class NormalizationExample {
    /** Run example. */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Normalization example started.");

            IgniteCache<Integer, Person> persons = createCache(ignite);

            // Defines first preprocessor that extracts features from an upstream data.
            IgniteBiFunction<Integer, Person, double[]> featureExtractor = (k, v) -> new double[] {
                v.getAge(),
                v.getSalary()
            };

            // Defines second preprocessor that normalizes features.
            IgniteBiFunction<Integer, Person, double[]> preprocessor = new NormalizationTrainer<Integer, Person>()
                .withP(1)
                .fit(ignite, persons, featureExtractor);

            // Creates a cache based simple dataset containing features and providing standard dataset API.
            try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(ignite, persons, preprocessor)) {
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

            System.out.println(">>> Normalization example completed.");
        }
    }

    /** */
    private static IgniteCache<Integer, Person> createCache(Ignite ignite) {
        CacheConfiguration<Integer, Person> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName("PERSONS");
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 2));

        IgniteCache<Integer, Person> persons = ignite.createCache(cacheConfiguration);

        persons.put(1, new Person("Mike", 10, 20));
        persons.put(2, new Person("John", 20, 10));
        persons.put(3, new Person("George", 30, 0));
        persons.put(4, new Person("Karl", 25, 15));

        return persons;
    }
}
