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

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Thin client async cache tests.
 */
public class AsyncCacheTest {
    // TODO: Add async test to ThinClientPartitionAwarenessStableTopologyTest
    // TODO: Add test tp FunctionalTest? The problem - they start new server in every test.

    /**
     * Tests IgniteFuture state transitions with getAsync.
     *
     * - Start and async operation
     * - Check that IgniteFuture is not done initially
     * - Wait for operation completion
     * - Verify that listener callback has been called
     * - Verify that operation result is correct
     *
     * @throws Exception
     */
    @Test
    public void testGetAsyncReportsCorrectIgniteFutureStates() throws Exception {
        try (LocalIgniteCluster ignored = LocalIgniteCluster.start(1);
             IgniteClient client = Ignition.startClient(getClientConfiguration())
        ) {
            ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName("testGetAsync");

            Person val = new Person(1, Integer.toString(1));
            ClientCache<Integer, Person> cache = client.getOrCreateCache(cacheCfg);
            cache.put(1, val);

            IgniteFuture<Person> fut = cache.getAsync(1);
            assertFalse(fut.isDone());

            AtomicBoolean listenerCalled = new AtomicBoolean(false);
            fut.listen(f -> {
                // TODO: Check completion thread! We should not run user code on the socket receiver thread!
                listenerCalled.set(true);
            });

            Person res = fut.get();
            assertEquals("1", res.getName());
            assertTrue(listenerCalled.get());
            assertTrue(fut.isDone());
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
