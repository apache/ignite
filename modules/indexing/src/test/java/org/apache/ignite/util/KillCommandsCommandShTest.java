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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteUuid;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelComputeTask;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelService;

/** Tests cancel of user created entities via control.sh. */
public class KillCommandsCommandShTest extends GridCommandHandlerClusterByClassAbstractTest {
    /** */
    private static List<IgniteEx> srvs;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        srvs = new ArrayList<>();

        for (int i = 0; i < SERVER_NODE_CNT; i++)
            srvs.add(grid(i));

        awaitPartitionMapExchange();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // No-op. Prevent cache destroy from super class.
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelComputeTask() throws Exception {
        doTestCancelComputeTask(client, srvs, sessId -> {
            int res = execute("--kill", "compute", sessId);

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        doTestCancelService(client, client, srvs.get(0), name -> {
            int res = execute("--kill", "service", name);

            assertEquals(EXIT_CODE_OK, res);
        });
    }

    /** */
    @Test
    public void testCancelUnknownComputeTask() {
        int res = execute("--kill", "compute", IgniteUuid.randomUuid().toString());

        assertEquals(EXIT_CODE_OK, res);
    }

    /** */
    @Test
    public void testCancelUnknownService() {
        int res = execute("--kill", "service", "unknown");

        assertEquals(EXIT_CODE_OK, res);
    }
}
