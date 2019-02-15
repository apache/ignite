/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
