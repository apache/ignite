/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.apache.ignite.ml.preprocessing.maxabsscaling.MaxAbsScalerTrainer;

/**
 * Example that shows how to use MaxAbsScaler preprocessor to scale the given data.
 *
 * Machine learning preprocessors are built as a chain. Most often a first preprocessor is a feature extractor as shown
 * in this example. The second preprocessor here is a MinMaxScaler preprocessor which is built on top of the feature
 * extractor and represents a chain of itself and the underlying feature extractor.
 */
public class MaxAbsScalerExample {
    /** Run example. */
    public static void main(String[] args) throws Exception {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Max abs example started.");

            IgniteCache<Integer, Person> persons = createCache(ignite);

            // Defines first preprocessor that extracts features from an upstream data.
            IgniteBiFunction<Integer, Person, Vector> featureExtractor = (k, v) -> VectorUtils.of(
                v.getAge(),
                v.getSalary()
            );

            // Defines second preprocessor that processes features.
            IgniteBiFunction<Integer, Person, Vector> preprocessor = new MaxAbsScalerTrainer<Integer, Person>()
                .fit(ignite, persons, featureExtractor);

            // Creates a cache based simple dataset containing features and providing standard dataset API.
            try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(ignite, persons, preprocessor)) {
                new DatasetHelper(dataset).describe();
            }

            System.out.println(">>> Max abs example completed.");
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
}
