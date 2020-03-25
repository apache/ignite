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
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.util.KillCommandsSQLTest.execute;
import static org.junit.Assert.assertTrue;

/**
 * General tests for the cancel command.
 */
class KillCommandsTests {
    /** Operations timeout. */
    public static final int TIMEOUT = 10_000;

    /** Latch to block compute task execution. */
    private static CountDownLatch computeLatch;

    /**
     * Test cancel of the compute task.
     *
     * @param cli Client node that starts tasks.
     * @param srvs Server nodes.
     * @param qryCanceler Query cancel closure.
     */
    public static void doTestCancelComputeTask(IgniteEx cli, List<IgniteEx> srvs, Consumer<String> qryCanceler)
        throws Exception {
        computeLatch = new CountDownLatch(1);

        IgniteFuture<Collection<Integer>> fut = cli.compute().broadcastAsync(() -> {
            computeLatch.await();

            return 1;
        });

        try {
            String[] id = new String[1];

            boolean res = waitForCondition(() -> {
                for (IgniteEx srv : srvs) {
                    List<List<?>> tasks = execute(srv, "SELECT SESSION_ID FROM SYS.JOBS");

                    if (tasks.size() == 1)
                        id[0] = (String)tasks.get(0).get(0);
                    else
                        return false;
                }

                return true;
            }, TIMEOUT);

            assertTrue(res);

            qryCanceler.accept(id[0]);

            for (IgniteEx srv : srvs) {
                res = waitForCondition(() -> {
                    List<List<?>> tasks = execute(srv, "SELECT SESSION_ID FROM SYS.JOBS");

                    return tasks.isEmpty();
                }, TIMEOUT);

                assertTrue(srv.configuration().getIgniteInstanceName(), res);
            }

            assertThrowsWithCause(() -> fut.get(TIMEOUT), IgniteException.class);
        } finally {
            computeLatch.countDown();
        }
    }
}
