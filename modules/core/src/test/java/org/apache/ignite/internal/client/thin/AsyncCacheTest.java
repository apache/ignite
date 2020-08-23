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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.client.IgniteClientFuture;
import org.apache.ignite.client.Person;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Thin client async cache tests.
 */
public class AsyncCacheTest extends AbstractThinClientTest {
    // TODO: Add async test to ThinClientPartitionAwarenessStableTopologyTest

    /** Client. */
    private static IgniteClient client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
        client = startClient(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        client.close();
        stopAllGrids();
    }

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
        ClientCacheConfiguration cacheCfg = new ClientCacheConfiguration().setName("testGetAsync");

        Person val = new Person(1, Integer.toString(1));
        ClientCache<Integer, Person> cache = client.getOrCreateCache(cacheCfg);
        cache.put(1, val);

        IgniteClientFuture<Person> fut = cache.getAsync(1);
        assertFalse(fut.isDone());

        AtomicReference<String> completionThreadName = new AtomicReference<>();
        fut.thenRun(() -> completionThreadName.set(Thread.currentThread().getName()));

        Person res = fut.get();
        assertEquals("1", res.getName());
        assertTrue(fut.isDone());

        assertNotNull(completionThreadName.get());
        assertFalse("Async operation should not complete on thin client listener thread",
                completionThreadName.get().startsWith("thin-client-channel"));
    }
}
