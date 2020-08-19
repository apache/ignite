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

package org.apache.ignite.client;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.lang.IgniteFuture;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Thin client async cache tests.
 */
public class AsyncCacheTest {
    /**
     * TODO
     *
     * @throws Exception
     */
    @Test
    public void testGetAsync() throws Exception {
        try (LocalIgniteCluster ignored = LocalIgniteCluster.start(2);
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName("testGetAsync");

            Person val = new Person(1, Integer.toString(1));
            ClientCache<Integer, Person> cache = client.getOrCreateCache(cacheCfg);
            cache.put(1, val);

            IgniteFuture<Person> fut = cache.getAsync(1);
            Person res = fut.get();
            assertEquals("1", res.getName());
        }
    }

    /** */
    private static ClientConfiguration getClientConfiguration() {
        return new ClientConfiguration()
                .setAddresses(Config.SERVER)
                .setSendBufferSize(0)
                .setReceiveBufferSize(0);
    }
}
