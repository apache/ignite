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

package org.apache.ignite.p2p;

import java.net.URL;
import java.util.UUID;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;

import static org.apache.ignite.configuration.DeploymentMode.CONTINUOUS;
import static org.apache.ignite.configuration.DeploymentMode.ISOLATED;
import static org.apache.ignite.configuration.DeploymentMode.PRIVATE;
import static org.apache.ignite.configuration.DeploymentMode.SHARED;

/**
 * Tests that a server peer-loads classes only from the client nodes that really have them, when thick clients with
 * partially overlapping class paths submit tasks in SHARED (and other) deployment modes.
 */
@GridCommonTest(group = "P2P")
public class GridP2PClientDeploymentSourceTest extends GridCommonAbstractTest {
    /** Task available only on the first client. */
    private static final String TASK1_CLS_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1";

    /** Job of {@link #TASK1_CLS_NAME}, available only on the first client. */
    private static final String JOB1_CLS_NAME = TASK1_CLS_NAME + "$TestJob1";

    /** Task available on both clients. */
    private static final String TASK2_CLS_NAME = "org.apache.ignite.tests.p2p.P2PTestTaskExternalPath2";

    /** Message logged by {@code GridDeploymentClassLoader} when a node reports a class as missing. */
    private static final String CLS_NOT_FOUND_MSG = "Failed to find class on remote node";

    /** Server node. */
    private static final int SRV = 0;

    /** Client with full class path. */
    private static final int FULL_CLI = 1;

    /** Client without {@link #TASK1_CLS_NAME} and {@link #JOB1_CLS_NAME}. */
    private static final int PARTIAL_CLI = 2;

    /** Class loader having all test classes. */
    private ClassLoader fullLdr;

    /** Class loader lacking task 1 and its job. */
    private ClassLoader partialLdr;

    /** Current deployment mode. */
    private DeploymentMode depMode;

    /** Server log. */
    private ListeningTestLogger srvLog;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDeploymentMode(depMode);
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setCacheConfiguration();

        if (getTestIgniteInstanceName(SRV).equals(igniteInstanceName))
            cfg.setGridLogger(srvLog);
        else if (getTestIgniteInstanceName(FULL_CLI).equals(igniteInstanceName))
            cfg.setClassLoader(fullLdr);
        else if (getTestIgniteInstanceName(PARTIAL_CLI).equals(igniteInstanceName))
            cfg.setClassLoader(partialLdr);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        URL[] urls = {new URL(GridTestProperties.getProperty("p2p.uri.cls"))};

        fullLdr = new GridTestExternalClassLoader(urls);
        partialLdr = new GridTestExternalClassLoader(urls, TASK1_CLS_NAME, JOB1_CLS_NAME);

        srvLog = new ListeningTestLogger(log);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Scenario (SHARED mode):
     * <ol>
     *     <li>Client 1 (has task 1, job 1 and task 2) executes task 2 on the server.</li>
     *     <li>Client 2 (has task 2 only) executes task 2 on the server and joins the shared deployment created by
     *     the first client, since the deployment is already able to load task 2.</li>
     *     <li>Client 1 executes task 1. The server must load job 1 from client 1, not fail because client 2
     *     (a participant of the same shared deployment) does not have it.</li>
     * </ol>
     */
    @Test
    public void testNeighbourClientWithoutJobClass() throws Exception {
        depMode = SHARED;

        LogListener clsNotFoundLsnr = LogListener.matches(CLS_NOT_FOUND_MSG).build();

        srvLog.registerListener(clsNotFoundLsnr);

        IgniteEx srv = startGrid(SRV);
        IgniteEx fullCli = startClientGrid(FULL_CLI);
        IgniteEx partialCli = startClientGrid(PARTIAL_CLI);

        UUID srvId = srv.localNode().id();

        Class task2Full = fullLdr.loadClass(TASK2_CLS_NAME);
        Class task2Partial = partialLdr.loadClass(TASK2_CLS_NAME);
        Class task1Full = fullLdr.loadClass(TASK1_CLS_NAME);

        Integer ldr2Full = (Integer)fullCli.compute().execute(task2Full, srvId);
        Integer ldr2Partial = (Integer)partialCli.compute().execute(task2Partial, srvId);

        // Both clients share the same deployment (and thus class loader) on the server in SHARED mode.
        assertEquals(ldr2Full, ldr2Partial);

        // Job class must be loaded from the client that has it, no matter which participants joined the deployment.
        Integer ldr1Full = (Integer)fullCli.compute().execute(task1Full, srvId);

        assertEquals(ldr2Full, ldr1Full);

        assertFalse("Class was requested from a node that does not have it", clsNotFoundLsnr.check());
    }

    /** */
    @Test
    public void testSharedMode() throws Exception {
        checkOwnerLeft(SHARED);
    }

    /** */
    @Test
    public void testContinuousMode() throws Exception {
        checkOwnerLeft(CONTINUOUS);
    }

    /** */
    @Test
    public void testPrivateMode() throws Exception {
        checkOwnerLeft(PRIVATE);
    }

    /** */
    @Test
    public void testIsolatedMode() throws Exception {
        checkOwnerLeft(ISOLATED);
    }

    /**
     * Checks that after the client which originally deployed a task leaves, the task is still executable by another
     * client having the same class: the class requests are targeted at the remaining client.
     *
     * @param mode Deployment mode.
     */
    private void checkOwnerLeft(DeploymentMode mode) throws Exception {
        depMode = mode;

        IgniteEx srv = startGrid(SRV);
        IgniteEx fullCli = startClientGrid(FULL_CLI);
        IgniteEx partialCli = startClientGrid(PARTIAL_CLI);

        UUID srvId = srv.localNode().id();

        Class task2Full = fullLdr.loadClass(TASK2_CLS_NAME);
        Class task2Partial = partialLdr.loadClass(TASK2_CLS_NAME);

        fullCli.compute().execute(task2Full, srvId);
        partialCli.compute().execute(task2Partial, srvId);

        stopGrid(FULL_CLI);

        awaitPartitionMapExchange();

        // Class loader of the remaining client must be used now.
        partialCli.compute().execute(task2Partial, srvId);
    }
}
