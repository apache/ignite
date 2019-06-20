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

package org.apache.ignite.internal.managers.deployment;

import java.net.URL;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestExternalClassLoader;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.config.GridTestProperties;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.GridTopic.TOPIC_CLASSLOAD;

/**
 * Tests the processing of deployment request with an attempt to load a class with an unknown class name.
 */
public class DeploymentRequestOfUnknownClassProcessingTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_TOPIC_NAME = "TEST_TOPIC_NAME";

    /** */
    private static final String UNKNOWN_CLASS_NAME = "unknown.UnknownClassName";

    /** */
    private final ListeningTestLogger remNodeLog = new ListeningTestLogger(false, log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setPeerClassLoadingEnabled(true);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid(getConfiguration(getTestIgniteInstanceName(0)));

        IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(1));

        cfg.setGridLogger(remNodeLog);

        startGrid(cfg);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testResponseReceivingOnDeploymentRequestOfUnknownClass() throws Exception {
        IgniteEx locNode = grid(0);
        IgniteEx remNode = grid(1);

        // Register deployment on remote node for attemt to load class on request receiving
        GridTestExternalClassLoader ldr = new GridTestExternalClassLoader(new URL[] {
            new URL(GridTestProperties.getProperty("p2p.uri.cls"))
        });

        Class task = ldr.loadClass("org.apache.ignite.tests.p2p.P2PTestTaskExternalPath1");

        GridDeployment locDep = remNode.context().deploy().deploy(task, task.getClassLoader());

        final GridFutureAdapter<Void> testResultFut = new GridFutureAdapter<>();

        final LogListener remNodeLogLsnr = LogListener
            .matches(s -> s.startsWith("Failed to resolve class: " + UNKNOWN_CLASS_NAME)).build();

        remNodeLog.registerListener(remNodeLogLsnr);

        locNode.context().io().addMessageListener(TEST_TOPIC_NAME, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                try {
                    assertTrue(msg instanceof GridDeploymentResponse);

                    GridDeploymentResponse resp = (GridDeploymentResponse)msg;

                    assertFalse("Unexpected response result, success=" + resp.success(), resp.success());

                    String errMsg = resp.errorMessage();

                    assertNotNull("Response should contain an error message.", errMsg);

                    assertTrue("Response contains unexpected error message, errorMessage=" + errMsg,
                        errMsg.startsWith("Requested resource not found (ignoring locally): " + UNKNOWN_CLASS_NAME));

                    testResultFut.onDone();
                }
                catch (Error e) {
                    testResultFut.onDone(e);
                }
            }
        });

        GridDeploymentRequest req = new GridDeploymentRequest(TEST_TOPIC_NAME, locDep.classLoaderId(),
            UNKNOWN_CLASS_NAME, false);

        req.responseTopicBytes(U.marshal(locNode.context(), req.responseTopic()));

        locNode.context().io().sendToGridTopic(remNode.localNode(), TOPIC_CLASSLOAD, req, GridIoPolicy.P2P_POOL);

        // Сhecks that the expected response has been received.
        testResultFut.get(5_000, TimeUnit.MILLISECONDS);

        // Checks that error has been logged on remote node.
        assertTrue(remNodeLogLsnr.check());
    }
}
