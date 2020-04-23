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

package org.apache.ignite.internal.managers.communication;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteIoTestMessagesTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        startClientGrid(3);
        startClientGrid(4);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIoTestMessages() throws Exception {
        for (Ignite node : G.allGrids()) {
            IgniteKernal ignite = (IgniteKernal)node;

            List<ClusterNode> rmts = new ArrayList<>(ignite.cluster().forRemotes().nodes());

            assertEquals(4, rmts.size());

            for (ClusterNode rmt : rmts) {
                ignite.sendIoTest(rmt, new byte[1024], false);

                ignite.sendIoTest(rmt, new byte[1024], true);

                ignite.sendIoTest(rmts, new byte[1024], false);

                ignite.sendIoTest(rmts, new byte[1024], true);
            }
        }
    }
}
