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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE;
import static org.apache.ignite.internal.managers.communication.UnknownMessageException.NO_REG_MSG;

/** */
public class DiscoverySerializationExceptionTest extends GridCommonAbstractTest {
    /** */
    private static final String ERR_MSG = String.format(NO_REG_MSG, NotRegisteredMessage.class.getSimpleName());

    /** */
    private ListeningTestLogger lsnrLog;

    /** */
    private volatile int failNodeIdx;

    /** */
    private volatile CountDownLatch failureHandlerLatch;

    /** */
    private final LogListener errLsnr = LogListener.matches(ERR_MSG).build();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Listening only expected node log.
        if (getTestIgniteInstanceName(failNodeIdx).equals(igniteInstanceName)) {
            cfg.setGridLogger(lsnrLog)
                .setFailureHandler(new NoOpFailureHandler() {
                    @Override protected boolean handle(Ignite ignite, FailureContext fctx) {
                        assertEquals(FailureType.SYSTEM_WORKER_TERMINATION, fctx.type());
                        assertEquals(getTestIgniteInstanceName(failNodeIdx), ignite.configuration().getIgniteInstanceName());

                        assertNotNull(fctx.error());
                        assertEquals(ERR_MSG, fctx.error().getMessage());

                        failureHandlerLatch.countDown();

                        return true;
                    }
                });
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        stopAllGrids();

        lsnrLog = new ListeningTestLogger(log);

        lsnrLog.registerListener(errLsnr);
    }

    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DUMP_THREADS_ON_FAILURE, value = "fasle")
    public void testWriteExceptionLogged() throws Exception {
        failNodeIdx = 1;
        failureHandlerLatch = new CountDownLatch(1);

        startGrids(2);

        grid(failNodeIdx).context().discovery().sendCustomEvent(new NotRegisteredMessage(""));

        assertTrue("Failure handler must be invoked", failureHandlerLatch.await(1, TimeUnit.MINUTES));
        assertTrue("Error must be logged", errLsnr.check(30_000));
        assertTrue(grid(failNodeIdx).context().invalid());
    }

    /** */
    @Test
    public void testWriteExceptionLoggedOnClient() throws Exception {
        failNodeIdx = 3;

        startGrids(2);

        startClientGrid(failNodeIdx).context().discovery().sendCustomEvent(new NotRegisteredMessage(""));

        // Currently, client node doesn't call failure handler.
        assertTrue("Error must be logged", errLsnr.check(30_000));
    }
}
