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

package org.apache.ignite.tests.p2p.cache;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.tests.p2p.NoValueClassOnServerAbstractClient;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class CacheNoValueClassOnServerTestClient extends NoValueClassOnServerAbstractClient {
    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    private CacheNoValueClassOnServerTestClient(String[] args) throws Exception {
        super(args);
    }

    /** {@inheritDoc} */
    @Override protected void runTest() throws Exception {
        IgniteCache<Integer, Person> cache = ignite.cache("default");

        for (int i = 0; i < 100; i++)
            cache.put(i, new Person("name-" + i));

        for (int i = 0; i < 100; i++) {
            Person p = cache.get(i);

            if (p == null)
                throw new Exception("Null result key: " + i);

            String expName = "name-" + i;

            assertEquals(expName, p.getName());

            if (i % 10 == 0)
                System.out.println("Get expected value: " + p.name());
        }
    }

    /**
     * @param args Arguments.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        try (CacheNoValueClassOnServerTestClient client = new CacheNoValueClassOnServerTestClient(args)) {
            client.runTest();
        }
        catch (Throwable e) {
            System.out.println("Unexpected error: " + e);

            e.printStackTrace(System.out);

            System.exit(1);
        }
    }
}
