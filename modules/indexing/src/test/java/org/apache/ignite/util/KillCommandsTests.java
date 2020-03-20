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

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.metric.SqlViewExporterSpiTest;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * General tests for the cancel command.
 */
class KillCommandsTests {
    /** Cache name. */
    public static final String DEFAULT_CACHE_NAME = "default";

    /** Page size. */
    public static final int PAGE_SZ = 5;

    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;

    /**
     * Test cancel of the compute task.
     *
     * @param cli Client node.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelComputeTask(IgniteEx cli, List<IgniteEx> srvs, Consumer<String> qryCanceler) throws Exception {
        IgniteFuture<Collection<Integer>> fut = cli.compute().broadcastAsync(() -> {
            Thread.sleep(10 * TIMEOUT);

            fail("Task should be killed!");

            return 1;
        });

        String[] id = new String[1];

        boolean res = waitForCondition(() -> {
            List<List<?>> tasks = SqlViewExporterSpiTest.execute(srvs.get(0), "SELECT SESSION_ID FROM SYS.JOBS");

            if (tasks.size() == 1) {
                id[0] = (String)tasks.get(0).get(0);

                return true;
            }

            return false;
        }, TIMEOUT);

        assertTrue(res);

        qryCanceler.accept(id[0]);

        for (IgniteEx srv : srvs) {
            res = waitForCondition(() -> {
                List<List<?>> tasks = SqlViewExporterSpiTest.execute(srv, "SELECT SESSION_ID FROM SYS.JOBS");

                return tasks.isEmpty();
            }, TIMEOUT);

            assertTrue(res);
        }

        assertThrowsWithCause(() -> fut.get(TIMEOUT), IgniteException.class);
    }
}
