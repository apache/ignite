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

import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class IgniteFailureProcessor {
    /** Instance. */
    public static final IgniteFailureProcessor INSTANCE = new IgniteFailureProcessor();

    /** Stop timeout milliseconds. */
    private static final long STOP_TIMEOUT_MS = 60 * 1000;

    /** Lock. */
    private final Object lock = new Object();

    /**
     * @param failureCtx Failure context.
     */
    public void processFailure(IgniteFailureContext failureCtx) {
        assert failureCtx != null;

        final GridKernalContext ctx = failureCtx.context();

        IgniteFailureHandler hnd = ctx.config().getIgniteFailureHandler();

        if (hnd == null)
            hnd = IgniteFailureHandler.DFLT_HND;

        final IgniteFailureAction act;

        synchronized (lock) {
            if (ctx.invalidated())
                return;

            act = hnd.onFailure(failureCtx);

            if (act != IgniteFailureAction.NOOP)
                ctx.invalidate();
        }

        switch (act) {
            case RESTART_JVM:
                restartJvm(failureCtx);

                break;

            case STOP:
                stopNode(failureCtx);

                break;

            default:
                assert act == IgniteFailureAction.NOOP : "Unsupported ignite failure action value: " + act;
        }
    }

    /** Restarts JVM. */
    public void restartJvm(final IgniteFailureContext failureCtx) {
        assert failureCtx != null;

        new Thread(
            new Runnable() {
                @Override public void run() {
                    final GridKernalContext ctx = failureCtx.context();

                    final IgniteLogger log = ctx.log(getClass());

                    U.warn(log, "Restarting JVM on Ignite failure of type " + failureCtx.type());

                    ctx.failure(failureCtx.type());

                    G.restart(true);
                }
            },
            "node-restarter"
        ).start();
    }

    /** Stops local node. */
    public void stopNode(final IgniteFailureContext failureCtx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    final GridKernalContext ctx = failureCtx.context();

                    final IgniteLogger log = ctx.log(getClass());

                    U.warn(log, "Stopping local node on Ignite failure of type " + failureCtx.type());

                    ctx.failure(failureCtx.type());

                    IgnitionEx.stop(ctx.igniteInstanceName(), true, true, STOP_TIMEOUT_MS);
                }
            },
            "node-stopper"
        ).start();
    }
}
