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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Handler will try to stop node if {@code tryStop} value is {@code true}.
 * If node can't be stopped during provided {@code timeout} or {@code tryStop} value is {@code false}
 * then JVM process will be terminated forcibly using {@code Runtime.getRuntime().halt()}.
 */
public class StopNodeOrHaltFailureHandler extends AbstractFailureHandler {
    /** Try stop. */
    private final boolean tryStop;

    /** Timeout. */
    private final long timeout;

    /**
     * Default constructor.
     */
    public StopNodeOrHaltFailureHandler() {
        this(false, 0);
    }

    /**
     * @param tryStop Try stop.
     * @param timeout Stop node timeout.
     */
    public StopNodeOrHaltFailureHandler(boolean tryStop, long timeout) {
        this.tryStop = tryStop;
        this.timeout = timeout;
    }

    /** {@inheritDoc} */
    @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
        IgniteLogger log = ignite.log();

        if (tryStop) {
            final CountDownLatch latch = new CountDownLatch(1);

            new Thread(
                new Runnable() {
                    @Override public void run() {
                        U.error(log, "Stopping local node on Ignite failure: [failureCtx=" + failureCtx + ']');

                        IgnitionEx.stop(ignite.name(), true, true);

                        latch.countDown();
                    }
                },
                "node-stopper"
            ).start();

            new Thread(
                new Runnable() {
                    @Override public void run() {
                        try {
                            if (!latch.await(timeout, TimeUnit.MILLISECONDS)) {
                                U.error(log, "Stopping local node timeout, JVM will be halted.");

                                Runtime.getRuntime().halt(Ignition.KILL_EXIT_CODE);
                            }
                        }
                        catch (InterruptedException e) {
                            // No-op.
                        }
                    }
                },
                "jvm-halt-on-stop-timeout"
            ).start();
        }
        else {
            U.error(log, "JVM will be halted immediately due to the failure: [failureCtx=" + failureCtx + ']');

            Runtime.getRuntime().halt(Ignition.KILL_EXIT_CODE);
        }

        return true;
    }

    /**
     * Get stop node timeout.
     *
     * @return Stop node timeout.
     */
    public long timeout() {
        return timeout;
    }

    /**
     * Get try stop.
     *
     * @return Try stop.
     */
    public boolean tryStop() {
        return tryStop;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(StopNodeOrHaltFailureHandler.class, this, "super", super.toString());
    }
}
