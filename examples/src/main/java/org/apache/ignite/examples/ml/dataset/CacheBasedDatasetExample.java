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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ml.util.DatasetHelper;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.dataset.primitive.SimpleDataset;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * Example that shows how to create dataset based on an existing Ignite Cache and then use it to calculate {@code mean}
 * and {@code std} values as well as {@code covariance} and {@code correlation} matrices.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with simple test data.</p>
 * <p>
 * After that it creates the dataset based on the data in the cache and uses Dataset API to find and output various
 * statistical metrics of the data.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this functionality further.</p>
 */
public class CacheBasedDatasetExample {
    /**
     * Run example.
     */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Cache Based Dataset example started.");

            IgniteCache<Integer, Vector> persons = null;
            try {
                persons = createCache(ignite);

                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<>(1, 2);

                // Creates a cache based simple dataset containing features and providing standard dataset API.
                try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(
                    ignite,
                    persons,
                    vectorizer
                )) {
                    new DatasetHelper(dataset).describe();
                }

                System.out.println(">>> Cache Based Dataset example completed.");
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
