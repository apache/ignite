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

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCacheConfiguration;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Checks that a cache with an empty cache group name is rejected without affecting the cluster.
 */
public class CacheGroupNameValidationTest extends GridCommonAbstractTest {
    /** Expected error message. */
    private static final String ERR_MSG = "Cache group name must not be empty.";

    /** Critical failures detected on all nodes. */
    private final List<FailureContext> failures = new CopyOnWriteArrayList<>();

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return (ignite, failureCtx) -> {
            failures.add(failureCtx);

            return false;
        };
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        failures.clear();

        super.afterTest();
    }

    /** @throws Exception If failed. */
    @Test
    public void testCreateCache() throws Exception {
        startGrids(2);

        IgniteEx cli = startClientGrid(2);

        Throwable e = assertThrowsWithCause(
            () -> cli.createCache(new CacheConfiguration<>("cache1").setGroupName("")),
            IllegalArgumentException.class);

        assertTrue(X.getFullStackTrace(e), X.hasCause(e, ERR_MSG, IllegalArgumentException.class));

        checkClusterOperational();
    }

    /** @throws Exception If failed. */
    @Test
    public void testThinClientCreateCache() throws Exception {
        startGrids(2);

        try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            Throwable e = assertThrowsWithCause(
                () -> cli.createCache(new ClientCacheConfiguration().setName("cache1").setGroupName("")),
                ClientException.class);

            assertTrue(e.getMessage(), e.getMessage().contains(ERR_MSG));
        }

        checkClusterOperational();
    }

    /** @throws Exception If failed. */
    @Test
    public void testStaticConfiguration() throws Exception {
        startGrid(0);

        Throwable e = assertThrowsWithCause(
            () -> startGrid(getConfiguration(getTestIgniteInstanceName(1))
                .setCacheConfiguration(new CacheConfiguration<>("cache1").setGroupName(""))),
            IllegalArgumentException.class);

        assertTrue(X.getFullStackTrace(e), X.hasCause(e, ERR_MSG, IllegalArgumentException.class));

        checkClusterOperational();
    }

    /** Checks that no critical failures happened, the invalid cache was not created, and a valid one can be. */
    private void checkClusterOperational() {
        assertTrue("Critical failures: " + failures, failures.isEmpty());

        IgniteEx srv = grid(0);

        assertNull(srv.cache("cache1"));

        srv.createCache(new CacheConfiguration<>("cache2").setBackups(1)).put(1, 1);

        assertEquals(1, srv.cache("cache2").get(1));
    }
}
