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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_OK;
import static org.apache.ignite.internal.commandline.CommandHandler.EXIT_CODE_UNEXPECTED_ERROR;

/** Test for checkpoint in control.sh command. */
public class GridCommandHandlerCheckpointTest extends GridCommandHandlerAbstractTest {
    /** */
    private final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    private final LogListener checkpointFinishedLsnr = LogListener.matches("Checkpoint finished").build();

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (!persistenceEnable())
            cfg.setDataStorageConfiguration(null);

        listeningLog.registerListener(checkpointFinishedLsnr);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();
        stopAllGrids();
        cleanPersistenceDir();
        injectTestSystemOut();
    }

    /** Test checkpoint command with persistence enabled. */
    @Test
    public void testCheckpointPersistenceCluster() throws Exception {
        persistenceEnable(true);

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");
        srv.cluster().state(ClusterState.ACTIVE);

        IgniteCache<Integer, Integer> cacheSrv = srv.getOrCreateCache(DEFAULT_CACHE_NAME);
        IgniteCache<Integer, Integer> cacheCli = cli.getOrCreateCache(DEFAULT_CACHE_NAME);

        cacheSrv.put(1, 1);
        cacheCli.put(1, 1);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint"));

        outputContains("Checkpoint triggered on all nodes");
        assertTrue(GridTestUtils.waitForCondition(checkpointFinishedLsnr::check, 10_000));

        String out = testOut.toString();
        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));

        testOut.reset();
        checkpointFinishedLsnr.reset();

        cacheSrv.put(2, 2);
        cacheCli.put(2, 2);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--reason", "test_reason"));

        outputContains("Checkpoint triggered on all nodes");
        assertTrue(GridTestUtils.waitForCondition(checkpointFinishedLsnr::check, 10_000));

        testOut.reset();
        checkpointFinishedLsnr.reset();

        cacheSrv.put(3, 3);
        cacheCli.put(3, 3);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish"));

        outputContains("Checkpoint triggered on all nodes");
        assertTrue(GridTestUtils.waitForCondition(checkpointFinishedLsnr::check, 10_000));

        checkpointFinishedLsnr.reset();
    }

    /** Test checkpoint command with in-memory cluster. */
    @Test
    public void testCheckpointInMemoryCluster() throws Exception {
        persistenceEnable(false);

        IgniteEx srv = startGrids(2);
        IgniteEx cli = startClientGrid("client");
        srv.cluster().state(ClusterState.ACTIVE);

        srv.createCache("testCache");
        assertEquals(EXIT_CODE_UNEXPECTED_ERROR, execute("--checkpoint"));

        outputContains("Can't checkpoint on in-memory node");
        assertFalse(checkpointFinishedLsnr.check());

        String out = testOut.toString();
        assertFalse(out.contains(cli.localNode().id().toString()));
        assertFalse(out.contains(Objects.toString(cli.localNode().consistentId())));

        checkpointFinishedLsnr.reset();
    }

    /** Test checkpoint with timeout. */
    @Test
    public void testCheckpointTimeout() throws Exception {
        persistenceEnable(true);

        IgniteEx srv = startGrids(1);
        srv.cluster().state(ClusterState.ACTIVE);

        assertEquals(EXIT_CODE_OK, execute("--checkpoint", "--wait-for-finish", "--timeout", "1000"));

        outputContains("Checkpoint triggered on all nodes");
        assertTrue(checkpointFinishedLsnr.check());

        checkpointFinishedLsnr.reset();
    }

    /** */
    private void outputContains(String regexp) {
        assertTrue(Pattern.compile(regexp).matcher(testOut.toString()).find());
    }
}
