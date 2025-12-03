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

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;

/** Test for checkpoint in control.sh command. */
public class GridCommandHandlerCheckpointTest extends GridCommandHandlerAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();
        cleanPersistenceDir();
        injectTestSystemOut();
        super.beforeTest();
    }

    /** */
    @Test
    public void testCheckpoint() throws Exception {
        IgniteEx srv = startGrids(2);
        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "test_reason"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish", "--timeout", "10000"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));

        testOut.reset();

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "planned", "--wait-for-finish", "--timeout", "5000"));
        assertTrue(testOut.toString().contains("Checkpoint triggered on all nodes"));
    }
}
