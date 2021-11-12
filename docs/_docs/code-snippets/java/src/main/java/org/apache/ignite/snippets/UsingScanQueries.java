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
package org.apache.ignite.snippets;

import java.util.List;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteClosure;
import org.junit.jupiter.api.Test;

public class UsingScanQueries {

    @Test
    void localQuery() {
        try (Ignite ignite = Ignition.start()) {
            IgniteCache<Integer, Person> cache = ignite.getOrCreateCache("myCache");
            //tag::localQuery[]
            QueryCursor<Cache.Entry<Integer, Person>> cursor = cache
                    .query(new ScanQuery<Integer, Person>().setLocal(true));
            //end::localQuery[]
        }
    }

    @Test
    void executingScanQueriesExample() {
        try (Ignite ignite = Ignition.start()) {
            //tag::scanQry[]
            //tag::predicate[]
            //tag::transformer[]
            IgniteCache<Integer, Person> cache = ignite.getOrCreateCache("myCache");
            //end::scanQry[]
            //end::predicate[]
            //end::transformer[]

            Person person = new Person(1, "Vasya Ivanov");
            person.setSalary(2000);
            cache.put(1, person);
            //tag::scanQry[]

            QueryCursor<Cache.Entry<Integer, Person>> cursor = cache.query(new ScanQuery<>());
            //end::scanQry[]
            System.out.println("Scan query output:" + cursor.getAll().get(0).getValue().getName());

            //tag::predicate[]

            // Find the persons who earn more than 1,000.
            IgniteBiPredicate<Integer, Person> filter = (key, p) -> p.getSalary() > 1000;

            try (QueryCursor<Cache.Entry<Integer, Person>> qryCursor = cache.query(new ScanQuery<>(filter))) {
                qryCursor.forEach(
                        entry -> System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue()));
            }
            //end::predicate[]

            //tag::transformer[]

            // Get only keys for persons earning more than 1,000.
            List<Integer> keys = cache.query(new ScanQuery<>(
                    // Remote filter
                    (IgniteBiPredicate<Integer, Person>) (k, p) -> p.getSalary() > 1000),
                    // Transformer
                    (IgniteClosure<Cache.Entry<Integer, Person>, Integer>) Cache.Entry::getKey).getAll();
            //end::transformer[]

            System.out.println("Transformer example output:" + keys.get(0));
        }
    }
}
