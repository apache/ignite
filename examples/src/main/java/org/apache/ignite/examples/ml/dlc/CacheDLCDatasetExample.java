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

package org.apache.ignite.examples.ml.dlc;

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dlc.dataset.DLCDataset;
import org.apache.ignite.ml.dlc.dataset.transformer.DLCTransformers;
import org.apache.ignite.ml.dlc.impl.cache.CacheBasedDLCFactory;

/**
 * How to create a DLC dataset from an existing Ignite Cache?
 */
public class CacheDLCDatasetExample {
    /** Run example. */
    public static void main(String[] args) {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> D-Learn Cache Dataset example started.");

            IgniteCache<Integer, Person> persons = createCache(ignite);

            // Initialization of the d-learn context. After this step context cache will be created with partitions
            // placed on the same nodes as the upstream Ignite Cache.

            // Loading of the d-learn context. During this step data will be transferred from the upstream cache to
            // context cache with specified transformation (it will be performed locally because partitions are on the
            // same nodes). In this case for every partition in upstream cache will be created labeled dataset partition
            // and this new partition will be filled with help of specified feature and label extractors.
            DLCDataset<Integer, Person, ?> dataset = new CacheBasedDLCFactory<>(ignite, persons)
                .createDLC(
                    (data, size) -> null,
                    DLCTransformers.upstreamToDataset((k, v) -> new double[]{ v.age, v.salary }, 2),
                    DLCDataset::new
                );

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

            System.out.println(">>> D-Learn Cache Dataset example completed.");
        }
    }

    /** */
    private static IgniteCache<Integer, Person> createCache(Ignite ignite) {
        CacheConfiguration<Integer, Person> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("PERSONS");
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 2));
        IgniteCache<Integer, Person> persons = ignite.createCache(cacheConfiguration);
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
    }
}
