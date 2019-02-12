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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ml.dataset.model.Person;
import org.apache.ignite.examples.ml.util.DatasetHelper;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.primitive.SimpleDataset;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.imputing.ImputingStrategy;

/**
 * Example that shows how to use <a href="https://en.wikipedia.org/wiki/Imputation_(statistics)">Imputing</a>
 * preprocessor to impute the missing values in the given data with most frequent values.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with simple test data.</p>
 * <p>
 * After that it defines preprocessors that extract features from an upstream data and impute missing values.</p>
 * <p>
 * Finally, it creates the dataset based on the processed data and uses Dataset API to find and output
 * various statistical metrics of the data.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this functionality further.</p>
 */
public class ImputingWithMostFrequentValuesExample {
    /** Run example. */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Imputing with most frequent values example started.");

            IgniteCache<Integer, Person> persons = createCache(ignite);

            // Defines first preprocessor that extracts features from an upstream data.
            IgniteBiFunction<Integer, Person, Vector> featureExtractor = (k, v) -> VectorUtils.of(
                v.getAge(),
                v.getSalary()
            );

            // Defines second preprocessor that normalizes features.
            IgniteBiFunction<Integer, Person, Vector> preprocessor = new ImputerTrainer<Integer, Person>()
                .withImputingStrategy(ImputingStrategy.MOST_FREQUENT)
                .fit(ignite, persons, featureExtractor);

            // Creates a cache based simple dataset containing features and providing standard dataset API.
            try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(ignite, persons, preprocessor)) {
                new DatasetHelper(dataset).describe();
            }

            System.out.println(">>> Imputing with most frequent values example completed.");
        }
    }

    /** */
    private static IgniteCache<Integer, Person> createCache(Ignite ignite) {
        CacheConfiguration<Integer, Person> cacheConfiguration = new CacheConfiguration<>();

        cacheConfiguration.setName("PERSONS");
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 2));

        IgniteCache<Integer, Person> persons = ignite.createCache(cacheConfiguration);

        persons.put(1, new Person("Mike", 10, 1));
        persons.put(2, new Person("John", 20, 2));
        persons.put(3, new Person("George", 15, 1));
        persons.put(4, new Person("Piter", 25, Double.NaN));
        persons.put(5, new Person("Karl", Double.NaN, 1));
        persons.put(6, new Person("Gustaw", 20, 2));
        persons.put(7, new Person("Alex", 20, 3));
        return persons;
    }
}
