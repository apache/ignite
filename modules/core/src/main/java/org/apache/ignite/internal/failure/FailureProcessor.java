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

package org.apache.ignite.internal.failure;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;

/**
 * General failure processing API
 */
public class FailureProcessor extends GridProcessorAdapter {
    /** Default stop node timeout. */
    private static final long DFLT_STOP_NODE_TIMEOUT = 60 * 1000;

    /** Ignite. */
    @IgniteInstanceResource private Ignite ignite; // FIXME inject ignite instance into FailureHandler!

    /** Lock. */
    private final Object lock = new Object();

    /** Handler. */
    private volatile FailureHandler hnd;

    /**
     * @param ctx Context.
     */
    public FailureProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        FailureHandler hnd = ctx.config().getFailureHandler();

        if (hnd == null)
            hnd = getDefaultFailureHandler();

        this.hnd = hnd;
    }

    /**
     *
     */
    protected FailureHandler getDefaultFailureHandler() {
        return new StopNodeOrHaltFailureHandler(true, DFLT_STOP_NODE_TIMEOUT);
    }

    /**
     *
     */
    private FailureHandler handler() {
        FailureHandler hnd = this.hnd;

        if (hnd == null)
            hnd = getDefaultFailureHandler();

        return hnd;
    }

    /**
     * Processes some failure.
     * May cause node termination.
     *
     * @param failureCtx Failure context.
     */
    public synchronized void process(FailureContext failureCtx) {
        synchronized (lock) {
            if (ctx.invalidationCause() != null) // Node already terminating, no reason to process more errors.
                return;

            final FailureHandler hnd = handler();

            if (hnd.onFailure(failureCtx, ignite)) // FIXME do not pass ignite as parameter, just inject into handler!
                ctx.invalidate(failureCtx);
        }
    }
    /**
     * Restarts JVM.
     */
    public void restartJvm(final FailureContext failureCtx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    final IgniteLogger log = ctx.log(getClass());

                    U.warn(log, "Restarting JVM on Ignite failure: " + failureCtx);

                    G.restart(true);
                }
            },
            "node-restarter"
        ).start();
    }

    /**
     * Stops local node.
     */
    public void stopNode(final FailureContext failureCtx) {
        new Thread(
            new Runnable() {
                @Override public void run() {
                    final IgniteLogger log = ctx.log(getClass());

                    U.warn(log, "Stopping local node on Ignite failure: " + failureCtx);

                    IgnitionEx.stop(ctx.igniteInstanceName(), true, true, 60 * 1000);
                }
            },
            "node-stopper"
        ).start();
    }
}
