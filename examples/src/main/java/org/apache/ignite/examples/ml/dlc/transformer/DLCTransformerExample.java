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

package org.apache.ignite.examples.ml.dlc.transformer;

import com.github.fommil.netlib.BLAS;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dlc.dataset.transformer.DLCTransformers;
import org.apache.ignite.ml.dlc.impl.cache.CacheBasedDLCFactory;

/** How to transform DLC into algorithm-specific view? */
public class DLCTransformerExample {
    /** Run example. */
    public static void main(String[] args) {
        BLAS bas = BLAS.getInstance();



        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> D-Learn Cache Dataset example started.");

            IgniteCache<Integer, Person> persons = createCache(ignite);

            // Initialization of the d-learn context. After this step context cache will be created with partitions
            // placed on the same nodes as the upstream Ignite Cache.

            // Loading of the d-learn context. During this step data will be transferred from the upstream cache to
            // context cache with specified transformation (it will be performed locally because partitions are on the
            // same nodes). In this case for every partition in upstream cache will be created labeled dataset partition
            // and this new partition will be filled with help of specified feature and label extractors.

            AlgorithmSpecificDataset<Integer, Person> dataset = new CacheBasedDLCFactory<>(ignite, persons)
                .createDLC(
                    new AlgorithmSpecificReplicatedDataTransformer<>(),
                    DLCTransformers.<Integer, Person, AlgorithmSpecificReplicatedData>upstreamToLabeledDataset(
                        (k, v) -> new double[]{ v.age },
                        (k, v) -> v.salary,
                        1
                    ).andThen(new AlgorithmSpecificRecoverableDataTransformer()),
                    AlgorithmSpecificDataset::new
                );

            double[] x = new double[2];

            for (int iter = 0; iter < 100; iter++) {
                double[] gradient = dataset.makeGradientStep(x);
                System.out.println("Point : " + Arrays.toString(x) + ", gradient : " + Arrays.toString(gradient));
                for (int i = 0; i < x.length; i++)
                    x[i] -= 0.01 * gradient[i];
            }

            System.out.println("New point : " + Arrays.toString(x));

            System.out.println(">>> D-Learn Cache Dataset example completed.");
        }
    }

    /** */
    private static IgniteCache<Integer, Person> createCache(Ignite ignite) {
        CacheConfiguration<Integer, Person> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("PERSONS");
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 1));
        IgniteCache<Integer, Person> persons = ignite.createCache(cacheConfiguration);
        persons.put(1, new Person("Mike", 10, 100));
        persons.put(2, new Person("John", 20, 200));
        persons.put(3, new Person("George", 30, 300));
        persons.put(4, new Person("Karl", 40, 400));
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
