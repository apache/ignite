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

import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.*;

/** Test for checkpoint in control.sh command. */
public class GridCommandHandlerCheckpointTest extends GridCommandHandlerAbstractTest {
    /** */
    private int clusterState;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (clusterState == 1)
            cfg.setDataStorageConfiguration(null);
        else {
            cfg.setDataStorageConfiguration(new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                            .setPersistenceEnabled(true)));
        }
        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
        injectTestSystemOut();
        super.beforeTest();
    }

    /** Test checkpoint command with persistence enabled. */
    @Test
    public void testCheckpointPersistenceCluster() throws Exception {
        clusterState = 0; // PDS cluster.

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");

        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));

        String out = testOut.toString();
        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));
        assertTrue(out.contains("Checkpoint triggered on all nodes"));

        outputContains("Checkpoint finished");

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "test_reason"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));
        outputContains("Checkpoint finished");

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish", "--timeout", "30000"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));
        outputContains("Checkpoint finished");

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "planned", "--wait-for-finish", "--timeout", "5000"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));
        outputContains("Checkpoint finished");
    }

    /** Test checkpoint command with in-memory cluster. */
    @Test
    public void testCheckpointInMemoryCluster() throws Exception {
        clusterState = 1; // In-memory cluster.

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");

        srv.cluster().state(ClusterState.ACTIVE);

        srv.createCache("testCache");
        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--checkpoint"));

        String out = testOut.toString();
        assertTrue(out.contains("Can't checkpoint on in-memory node"));

        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));
    }

    /** */
    @Test
    public void testCheckpointTimeout() throws Exception {
        clusterState = 0; // PDS cluster.

        IgniteEx srv = startGrids(1);
        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish", "--timeout", "1000"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));
        outputContains("Checkpoint finished");
    }

    /** */
    private void outputContains(String regexp) {
        assertTrue(Pattern.compile(regexp).matcher(testOut.toString()).find());
    }
}
