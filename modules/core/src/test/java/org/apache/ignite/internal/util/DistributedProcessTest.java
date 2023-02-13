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

package org.apache.ignite.internal.util;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.util.distributed.DistributedProcess;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.distributed.DistributedProcess.DistributedProcessType.TEST_PROCESS;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** */
public class DistributedProcessTest extends GridCommonAbstractTest {
    /** */
    @Test
    public void testAssertionErrorHandled() throws Exception {
        int nodes = 3;

        startGrids(nodes);

        DistributedProcess<Integer, Integer> process = null;

        CountDownLatch finLatch = new CountDownLatch(nodes);

        for (int n = 0; n < nodes; n++) {
            int n0 = n;

            process = new DistributedProcess<>((grid(n)).context(), TEST_PROCESS,
                req -> runAsync(() -> {
                    assert n0 != 1;  // grid(1) fails processing request.

                    return 0;
                }),
                (uuid, res, err) -> {
                    assertEquals(nodes - 1, res.values().size());
                    assertEquals(1, err.size());
                    assertTrue(err.get(grid(1).localNode().id()) instanceof AssertionError);

                    finLatch.countDown();
                }
            );
        }

        process.start(UUID.randomUUID(), 0);

        assertTrue(finLatch.await(30, TimeUnit.SECONDS));
    }
}
