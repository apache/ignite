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
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thin client async cache tests.
 */
public class AsyncCacheTest extends AbstractThinClientTest {
    // TODO: Add async tests to all PartitionAwareness tests
    /** Default timeout value. */
    private static final long TIMEOUT = 1_000L;

    /** Client. */
    private static IgniteClient client;

    /** */
    private static ClientCache<Integer, Person> personCache;

    /** */
    private static ClientCache<Integer, Integer> intCache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrid(0);
        client = startClient(0);

        personCache = client.getOrCreateCache("personCache");
        intCache = client.getOrCreateCache("intCache");
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        client.close();
        stopAllGrids();
    }

    /**
     * Tests IgniteClientFuture state transitions with getAsync.
     *
     * - Start an async operation
     * - Check that IgniteFuture is not done initially
     * - Wait for operation completion
     * - Verify that listener callback has been called
     * - Verify that operation result is correct
     */
    @Test
    public void testGetAsyncReportsCorrectIgniteFutureStates() throws Exception {
        Person val = new Person(1, Integer.toString(1));
        personCache.put(1, val);

        IgniteClientFuture<Person> fut = personCache.getAsync(1);
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

    /**
     * Tests that async operation can be cancelled.
     *
     * - Start an async operation
     * - Check that cancel returns true and future becomes cancelled
     */
    @Test
    public void testGetAsyncCanBeCancelled() {
        intCache.put(1, 2);

        IgniteClientFuture<Integer> fut = intCache.getAsync(1);

        assertTrue(fut.cancel(true));
        assertTrue(fut.isCancelled());
        GridTestUtils.assertThrowsAnyCause(null, fut::get, CancellationException.class, null);
    }

    /**
     * Tests getAsync basic functionality.
     *
     * - key and val can't be null
     * - missing key returns null
     * - existing key returns value
     * - missing cache causes exception
     */
    @Test
    public void testGetAsyncFunctional() {
        // TODO
    }

    /**
     * Tests putAsync basic functionality.
     *
     * - key and val can't be null
     * - valid key and val result in cache update
     * - missing cache causes exception
     */
    @Test
    public void testPutAsyncFunctional() throws Exception {
        intCache.putAsync(1, 2);
        assertTrue(GridTestUtils.waitForCondition(() -> intCache.get(1) == 2, TIMEOUT));
    }
}
