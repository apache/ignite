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

package org.apache.ignite.internal.processors.security.sandbox;

import java.security.AccessControlException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteScheduler;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.lang.RunnableX;
import org.junit.Test;

/**
 * Checks that a scheduler closure is executed inside the sandbox on a remote node.
 */
public class SchedulerSandboxTest extends AbstractSandboxTest {
    /** Latch. */
    private static volatile CountDownLatch latch;

    /** Error. */
    private static volatile AccessControlException error;

    /** */
    @Test
    public void testRunLocal() {
        Consumer<IgniteScheduler> c = new Consumer<IgniteScheduler>() {
            @Override public void accept(IgniteScheduler s) {
                s.runLocal(schedulerRunnable()).get();
            }
        };

        testSchedulerOperation(c);
    }

    /** */
    @Test
    public void testRunLocalWithDelay() {
        Consumer<IgniteScheduler> c = new Consumer<IgniteScheduler>() {
            @Override public void accept(IgniteScheduler s) {
                s.runLocal(schedulerRunnable(), 1, TimeUnit.MILLISECONDS);
            }
        };

        testSchedulerOperation(c);
    }

    /** */
    @Test
    public void testCallLocal() {
        Consumer<IgniteScheduler> c = new Consumer<IgniteScheduler>() {
            @Override public void accept(IgniteScheduler s) {
                s.callLocal(() -> {
                    schedulerRunnable().run();

                    return null;
                });
            }
        };

        testSchedulerOperation(c);
    }

    /** */
    private void testSchedulerOperation(Consumer<IgniteScheduler> consumer) {
        execute(grid(CLNT_ALLOWED_WRITE_PROP), consumer, false);
        execute(grid(CLNT_FORBIDDEN_WRITE_PROP), consumer, true);
    }

    /** */
    private void execute(Ignite node, Consumer<IgniteScheduler> consumer, boolean isForbiddenCase) {
        RunnableX r = () -> node.compute().run(() -> {
            error = null;

            latch = new CountDownLatch(1);

            consumer.accept(Ignition.localIgnite().scheduler());

            try {
                latch.await(10, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            if (error != null)
                throw error;
        });

        if (isForbiddenCase)
            runForbiddenOperation(r, AccessControlException.class);
        else
            runOperation(r);
    }

    /** */
    private Runnable schedulerRunnable() {
        return () -> {
            try {
                controlAction();
            }
            catch (AccessControlException e) {
                error = e;
            }
            finally {
                latch.countDown();
            }
        };
    }
}
