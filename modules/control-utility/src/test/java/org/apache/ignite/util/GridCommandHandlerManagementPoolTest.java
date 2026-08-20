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

package org.apache.ignite.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlFunction;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.Ignition.startClient;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/**
 * Tests management pool usage for management tasks.
 */
public class GridCommandHandlerManagementPoolTest extends GridCommandHandlerClusterPerMethodAbstractTest {
    /** */
    private static final long TIMEOUT = 10_000L;

    /** */
    @Test
    public void testManagementTasksWorksWhenClientPoolBlocked() throws Exception {
        Ignite ignite = startGrid(0);

        assertEquals(EXIT_CODE_OK, execute("--set-state", ClusterState.ACTIVE.name()));

        ignite.createCache(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setSqlSchema(QueryUtils.DFLT_SCHEMA)
            .setSqlFunctionClasses(TestSqlFunctions.class)
            .setIndexedTypes(Integer.class, String.class)
        );

        TestSqlFunctions.latch = new CountDownLatch(1);

        try (IgniteClient client = startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            // Block client pool by SQL queries.
            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
                () -> client.query(new SqlFieldsQuery("SELECT wait_latch()")).getAll(),
                ClientConnectorConfiguration.DFLT_THREAD_POOL_SIZE, "client-thread");

            // Check that management tasks still can be processed.
            assertEquals(EXIT_CODE_OK, execute("--state")); // Native command.
            assertEquals(EXIT_CODE_OK, execute("--checkpoint")); // Multi-node task command.

            TestSqlFunctions.latch.countDown();

            fut.get(TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    /** */
    public static class TestSqlFunctions {
        /** */
        private static CountDownLatch latch;

        /** */
        @QuerySqlFunction
        public static boolean wait_latch() {
            try {
                latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignored) {
                return false;
            }

            return true;
        }
    }
}
