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

package org.apache.ignite.internal.util.nio;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Ignore;
import org.junit.Test;

/**
 * A manual test for an unlikely situation with async key cancellation (see javadocs on the test method).
 * The test is manual because the race is extremely rare for trying to reproduce it in each run of the tests.
 * Before running the test, apply the following patch to GridNioServer to increase the probability of the event:
 *
 * @@ -2269,7 +2269,7 @@
 *                      select = true;
 *
 *                      try {
 * -                        if (!changeReqs.isEmpty())
 * +                        if (!changeReqs.isEmpty() || Math.random() < 0.999)
 *                              continue;
 *
 *                          blockingSectionBegin();
 *
 * The test sometimes fails because the client thinks the cluster is unaccessible. Such runs should be ignored
 * and test just rerun.
 */
@Ignore("This is a manual test, see class javadoc")
public class FastSessionMoveAndKeyCancellationTest extends GridCommonAbstractTest {
    /** Logger used to intersept log messages. */
    private final ListeningTestLogger logger = new ListeningTestLogger(log);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientConnectorConfiguration(new ClientConnectorConfiguration().setSelectorCount(2));

        cfg.setGridLogger(logger);

        return cfg;
    }

    /**
     * A session might be moved from worker A and then back to it. CancelledKeyException might be thrown if no selection
     * operation happened before cancelling the key and trying to re-register the channel with the same selector.
     *
     * This test makes sure this does not happen.
     */
    @SuppressWarnings("unused")
    @Test
    @WithSystemProperty(key = GridNioServer.IGNITE_IO_BALANCE_RANDOM_BALANCE, value = "true")
    public void quickSessionMovingBetweenWorkersShouldNotTriggerCancelledKeyException() throws Exception {
        CountDownLatch failureLogMsgsLatch = new CountDownLatch(2);

        logger.registerListener(
            message -> {
                if (message.contains("Caught unhandled exception in NIO worker thread")
                    || message.contains("java.nio.channels.CancelledKeyException")) {
                    failureLogMsgsLatch.countDown();
                }
            }
        );

        try (
            IgniteEx ignite = startGrid(1);
            IgniteClient client1 = startClient()
        ) {
            IgniteInternalFuture<?> clientJobsFut = multithreadedAsync(new ReconnectConstantly(), 2);

            try {
                assertFalse("CancelledKeyException was thrown", failureLogMsgsLatch.await(1, TimeUnit.MINUTES));
            }
            finally {
                clientJobsFut.cancel();
            }
        }
    }

    /**
     * Starts a client.
     *
     * @return Client.
     */
    private static IgniteClient startClient() {
        return Ignition.startClient(new ClientConfiguration().setAddresses("127.0.0.1:10800"));
    }

    /**
     * Callable that just creates a new client and disconnects immediately, in a loop, until the thread is interrupted.
     */
    private static class ReconnectConstantly implements Callable<Object> {
        /** {@inheritDoc} */
        @SuppressWarnings("EmptyTryBlock")
        @Override public Object call() throws Exception {
            while (!Thread.currentThread().isInterrupted()) {
                try (IgniteClient ignored = startClient()) {
                    // do nothing, just let the client disconnect
                }
            }

            return null;
        }
    }
}
