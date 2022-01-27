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

package org.apache.ignite.internal.processors.security.scheduler;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.processors.security.AbstractRemoteSecurityContextCheckTest;
import org.junit.Test;

/**
 * Testing operation security context when the scheduler closure is executed on remote nodes.
 * <p>
 * The initiator node broadcasts a task to 'run' nodes that run a scheduler closure (runnable or callable). The closure
 * is executed on 'run' nodes and broadcasts a task to 'endpoint' nodes. On every step, it is performed verification
 * that operation security context is the initiator context.
 */
public class SchedulerRemoteSecurityContextCheckTest extends AbstractRemoteSecurityContextCheckTest {
    /** */
    private static volatile CountDownLatch latch;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridAllowAll(SRV_INITIATOR);

        startClientAllowAll(CLNT_INITIATOR);

        startGridAllowAll(SRV_RUN);

        startClientAllowAll(CLNT_RUN);

        startGridAllowAll(SRV_ENDPOINT);

        startClientAllowAll(CLNT_ENDPOINT);

        awaitPartitionMapExchange();
    }

    /** */
    @Test
    public void testRunLocal() {
        runAndCheck(() -> Ignition.localIgnite().scheduler()
            .runLocal(new TestClosure(endpointIds())).get());
    }

    /** */
    @Test
    public void testRunLocalWithDelay() {
        runAndCheck(() -> Ignition.localIgnite().scheduler()
            .runLocal(new TestClosure(endpointIds()), 1, TimeUnit.MILLISECONDS));
    }

    /** */
    @Test
    public void testCallLocal() {
        runAndCheck(() -> Ignition.localIgnite().scheduler()
            .callLocal(new TestClosure(endpointIds())).get());
    }

    /** {@inheritDoc} */
    @Override protected void setupVerifier(Verifier verifier) {
        nodesToRun().forEach(n -> {
            VERIFIER.expect(n, OPERATION_START, 1);
            VERIFIER.expect(n, OPERATION_CHECK, 1);
        });
        endpoints().forEach(n -> VERIFIER.expect(n, OPERATION_ENDPOINT, 2));
    }

    /** {@inheritDoc} */
    @Override protected void beforeCompute() {
        latch = new CountDownLatch(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterCompute() {
        try {
            latch.await(10, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /** Test closure. */
    private static class TestClosure implements Runnable, Callable<Void> {
        /** Endpoint ids. */
        private final Collection<UUID> endpointIds;

        /** */
        public TestClosure(Collection<UUID> endpointIds) {
            this.endpointIds = endpointIds;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            VERIFIER.register(OPERATION_CHECK);

            compute(Ignition.localIgnite(), endpointIds).broadcast(() -> VERIFIER.register(OPERATION_ENDPOINT));

            latch.countDown();
        }

        /** {@inheritDoc} */
        @Override public Void call() throws Exception {
            run();

            return null;
        }
    }
}
