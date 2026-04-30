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

package org.apache.ignite.spi.discovery.tcp;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jspecify.annotations.NonNull;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;

/** */
public class DiscoverySerializationExceptionLoggedTest extends GridCommonAbstractTest {
    /** */
    private static final String EXP_ERR_MSG = "No registration for class " + NotRegisteredMessage.class.getSimpleName();

    /** */
    private ListeningTestLogger lsnrLog;

    /** */
    private volatile boolean errMsgInFailureHandlerFound;

    /** */
    private volatile CountDownLatch failureHandlerLatch;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setGridLogger(lsnrLog)
            .setFailureHandler(new NoOpFailureHandler() {
                /** {@inheritDoc} */
                @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
                    errMsgInFailureHandlerFound = Objects.equals(EXP_ERR_MSG, failureCtx.error().getMessage());

                    failureHandlerLatch.countDown();

                    return false;
                }
            });
    }

    // TODO: check case when message registered on sender and NOT registered on receiver.
    // Both: server and client.

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        errMsgInFailureHandlerFound = false;

        lsnrLog = new ListeningTestLogger(log);

        startGrids(2);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "fasle")
    public void testSerdesExceptionLogged() throws Exception {
        LogListener serdesErrLsnr = errMessageListener();

        failureHandlerLatch = new CountDownLatch(1);

        grid(1).context().discovery().sendCustomEvent(new NotRegisteredMessage(""));

        assertTrue(failureHandlerLatch.await(getTestTimeout(), TimeUnit.MILLISECONDS));
        assertTrue(errMsgInFailureHandlerFound);
        assertTrue(serdesErrLsnr.check(getTestTimeout() / 2));
    }

    /** */
    @Test
    public void testSerdesExceptionLoggedOnClient() throws Exception {
        LogListener serdesErrLsnr = errMessageListener();

        startClientGrid(3).context().discovery().sendCustomEvent(new NotRegisteredMessage(""));

        // Client node doesn't call failure handler to prevent JVM stop on error.
        // Assume, user will write client fail handler manually.
        assertTrue(serdesErrLsnr.check(getTestTimeout() / 2));
    }

    /** */
    private @NonNull LogListener errMessageListener() {
        LogListener serdesErrLsnr = LogListener
            .matches(EXP_ERR_MSG)
            .build();

        lsnrLog.registerListener(serdesErrLsnr);

        return serdesErrLsnr;
    }
}