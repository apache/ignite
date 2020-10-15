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

package org.apache.ignite.failure;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests that taking new exchange task doesn't interpreted as blocked exchange thread.
 * If network timeout, used as default poll timeout in exchange worker queue, is greater than
 * system thread worker blocked timeout and exchange task is fast (few milliseconds),
 * exchange thread, waiting for tasks from queue, should not be considered as blocked.
 */
public class ExchangeWorkerWaitingForTaskTest extends GridCommonAbstractTest {
    /** */
    private static final long SYSTEM_WORKER_BLOCKED_TIMEOUT = 3000;

    /** */
    private final CompletableFuture<Void> falseBlockedExchangeFuture = new CompletableFuture<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setNetworkTimeout(2 * SYSTEM_WORKER_BLOCKED_TIMEOUT);
        cfg.setSystemWorkerBlockedTimeout(SYSTEM_WORKER_BLOCKED_TIMEOUT);

        cfg.setFailureHandler(new FailureHandler() {
            @Override public boolean onFailure(Ignite ignite, FailureContext ctx) {
                if (ctx.type() == FailureType.SYSTEM_WORKER_BLOCKED) {
                    String msg = ctx.error().getMessage();

                    if (msg.contains("partition-exchanger"))
                        falseBlockedExchangeFuture.complete(null);
                }

                return false;
            }
        });

        return cfg;
    }

    /** */
    @Test
    public void testHandlerNotReportFalseBlocking() throws Exception {
        startGrid(1);
        startGrid(2);

        GridTestUtils.assertThrows(
            log(),
            () -> falseBlockedExchangeFuture.get(2 * SYSTEM_WORKER_BLOCKED_TIMEOUT, TimeUnit.MILLISECONDS),
            TimeoutException.class,
            null);
    }
}
