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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.examples.ml.dataset.model.Person;
import org.apache.ignite.examples.ml.util.DatasetHelper;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.primitive.SimpleDataset;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;

/**
 * Example that shows how to create dataset based on an existing local storage and then use it to calculate {@code mean}
 * and {@code std} values as well as {@code covariance} and {@code correlation} matrices.
 * <p>
 * Code in this example the storage with simple test data.</p>
 * <p>
 * After that it creates the dataset based on the data in the storage and uses Dataset API to find and output
 * various statistical metrics of the data.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this functionality further.</p>
 */
public class LocalDatasetExample {
    /** Run example. */
    public static void main(String[] args) throws Exception {
        System.out.println(">>> Local Dataset example started.");

        Map<Integer, Person> persons = createCache();

        // Creates a local simple dataset containing features and providing standard dataset API.
        try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(
            persons,
            2,
            (k, v) -> VectorUtils.of(v.getAge(), v.getSalary())
        )) {
            new DatasetHelper(dataset).describe();
        }

        System.out.println(">>> Local Dataset example completed.");
    }

    /** */
    private static Map<Integer, Person> createCache() {
        Map<Integer, Person> persons = new HashMap<>();

        persons.put(1, new Person("Mike", 42, 10000));
        persons.put(2, new Person("John", 32, 64000));
        persons.put(3, new Person("George", 53, 120000));
        persons.put(4, new Person("Karl", 24, 70000));

        return persons;
    }
}
