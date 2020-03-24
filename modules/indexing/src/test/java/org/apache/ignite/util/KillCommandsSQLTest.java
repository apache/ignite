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
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;
import static org.apache.ignite.util.KillCommandsTests.doTestCancelService;

/** Tests cancel of user created entities via SQL. */
public class KillCommandsSQLTest extends GridCommonAbstractTest {
    /** */
    public static final int  NODES_CNT = 3;

    /** */
    public static final String KILL_SVC_QRY = "KILL SERVICE";

    /** */
    private static List<IgniteEx> srvs;

    /** */
    private static IgniteEx cli;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(NODES_CNT);

        srvs = new ArrayList<>();

        for (int i = 0; i < NODES_CNT; i++)
            srvs.add(grid(i));

        cli = startClientGrid("client");

        srvs.get(0).cluster().state(ACTIVE);
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelService() throws Exception {
        doTestCancelService(cli, srvs.get(0), name -> execute(cli, KILL_SVC_QRY + " '" + name + "'"));
    }

    /** @throws Exception If failed. */
    @Test
    public void testCancelUnknownService() throws Exception {
        assertThrowsWithCause(() -> execute(cli, KILL_SVC_QRY + " 'unknown'"),
            RuntimeException.class);
    }
}
