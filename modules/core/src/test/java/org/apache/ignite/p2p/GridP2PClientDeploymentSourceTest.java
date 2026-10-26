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
import org.apache.ignite.internal.managers.deployment.GridDeploymentPerVersionStore;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
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

    /** Log category the peer class loaders of a node log under. The miss on a participant is reported at DEBUG level. */
    private static final String DEP_LOG_NAME = GridDeploymentPerVersionStore.class.getName();

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

    /** Level of {@link #DEP_LOG_NAME} before the test. */
    private Level initDepLogLevel;

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

        initDepLogLevel = LoggerContext.getContext(false).getConfiguration().getLoggerConfig(DEP_LOG_NAME).getLevel();
        Configurator.setLevel(DEP_LOG_NAME, Level.DEBUG);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        Configurator.setLevel(DEP_LOG_NAME, initDepLogLevel);

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

    /**
     * Same as {@link #testNeighbourClientWithoutJobClass()} but the clients deploy in the opposite order, so the client
     * lacking the job class is the first participant of the shared deployment and is asked first:
     * <ol>
     *     <li>Client 2 (has task 2 only) executes task 2 on the server and creates the shared deployment.</li>
     *     <li>Client 1 (has task 1, job 1 and task 2) executes task 2 and joins that deployment after client 2.</li>
     *     <li>Client 1 executes task 1. The server asks client 2 for job 1 first, gets "not found" and must go on
     *     to client 1 instead of giving up on the first miss.</li>
     * </ol>
     */
    @Test
    public void testNeighbourClientWithoutJobClassAskedFirst() throws Exception {
        depMode = SHARED;

        IgniteEx srv = startGrid(SRV);
        IgniteEx fullCli = startClientGrid(FULL_CLI);
        IgniteEx partialCli = startClientGrid(PARTIAL_CLI);

        UUID srvId = srv.localNode().id();
        String partialCliId = partialCli.localNode().id().toString();

        // The log abbreviates packages, so match on the simple names. The trailing comma keeps the task name from
        // matching its job.
        String task1SimpleName = TASK1_CLS_NAME.substring(TASK1_CLS_NAME.lastIndexOf('.') + 1);
        String job1SimpleName = JOB1_CLS_NAME.substring(JOB1_CLS_NAME.lastIndexOf('.') + 1);

        // Client 2 has neither task 1 nor job 1. The server reports each miss on client 2 exactly once and then
        // goes on to the next node.
        LogListener taskMissedOnPartialLsnr = LogListener.matches(s -> s.contains(CLS_NOT_FOUND_MSG)
            && s.contains(task1SimpleName + ',')
            && s.contains(partialCliId)).times(1).build();

        LogListener jobMissedOnPartialLsnr = LogListener.matches(s -> s.contains(CLS_NOT_FOUND_MSG)
            && s.contains(job1SimpleName + ',')
            && s.contains(partialCliId)).times(1).build();

        // No other class is ever reported missing: task 2 is served by client 2, task 1 and job 1 by client 1.
        LogListener otherMissLsnr = LogListener.matches(s -> s.contains(CLS_NOT_FOUND_MSG)
            && !s.contains(task1SimpleName)).build();

        srvLog.registerListener(taskMissedOnPartialLsnr);
        srvLog.registerListener(jobMissedOnPartialLsnr);
        srvLog.registerListener(otherMissLsnr);

        Class task2Partial = partialLdr.loadClass(TASK2_CLS_NAME);
        Class task2Full = fullLdr.loadClass(TASK2_CLS_NAME);
        Class task1Full = fullLdr.loadClass(TASK1_CLS_NAME);

        Integer ldr2Partial = (Integer)partialCli.compute().execute(task2Partial, srvId);
        Integer ldr2Full = (Integer)fullCli.compute().execute(task2Full, srvId);

        // Client 1 joined the deployment created by client 2.
        assertEquals(ldr2Partial, ldr2Full);

        // Job 1 exists on client 1 only. The server asks client 2 first and must fall through to client 1.
        Integer ldr1Full = (Integer)fullCli.compute().execute(task1Full, srvId);

        assertEquals(ldr2Partial, ldr1Full);

        assertTrue("Task class was not requested from the first participant before the owner",
            taskMissedOnPartialLsnr.check());

        assertTrue("Job class was not requested from the first participant before the owner",
            jobMissedOnPartialLsnr.check());

        assertFalse("A class other than the job class was requested from a node that does not have it",
            otherMissLsnr.check());
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
