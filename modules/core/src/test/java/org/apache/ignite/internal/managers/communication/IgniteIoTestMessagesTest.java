/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
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
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class IgniteIoTestMessagesTest extends GridCommonAbstractTest {
    /** */
    private boolean client;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(3);

        client = true;

        startGrid(3);

        startGrid(4);
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
