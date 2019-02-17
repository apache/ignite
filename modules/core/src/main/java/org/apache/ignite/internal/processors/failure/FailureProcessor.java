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

package org.apache.ignite.internal.processors.failure;

import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.failure.FailureType.SYSTEM_WORKER_BLOCKED;

/**
 * General failure processing API
 */
public class FailureProcessor extends GridProcessorAdapter {
    /** Value of the system property that enables threads dumping on failure. */
    private static final boolean IGNITE_DUMP_THREADS_ON_FAILURE =
        IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_DUMP_THREADS_ON_FAILURE, true);

    /** Ignite. */
    private final Ignite ignite;

    /** Handler. */
    private volatile FailureHandler hnd;

    /** Failure context. */
    private volatile FailureContext failureCtx;

    /** Reserve buffer, which can be dropped to handle OOME. */
    private volatile byte[] reserveBuf;

    /**
     * @param ctx Context.
     */
    public FailureProcessor(GridKernalContext ctx) {
        super(ctx);

        this.ignite = ctx.grid();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        FailureHandler hnd = ctx.config().getFailureHandler();

        if (hnd == null)
            hnd = getDefaultFailureHandler();

        reserveBuf = new byte[IgniteSystemProperties.getInteger(
            IgniteSystemProperties.IGNITE_FAILURE_HANDLER_RESERVE_BUFFER_SIZE, 64 * 1024)];

        assert hnd != null;

        this.hnd = hnd;

        U.quietAndInfo(log, "Configured failure handler: [hnd=" + hnd + ']');
    }

    /**
     * This method is used to initialize local failure handler if {@link IgniteConfiguration} don't contain configured one.
     *
     * @return Default {@link FailureHandler} implementation.
     */
    protected FailureHandler getDefaultFailureHandler() {
        FailureHandler hnd = new StopNodeOrHaltFailureHandler();

        hnd.setIgnoredFailureTypes(Collections.singleton(SYSTEM_WORKER_BLOCKED));

        return hnd;
    }

    /**
     * @return Failure context.
     */
    public FailureContext failureContext() {
        return failureCtx;
    }

    /**
     * Processes failure accordingly to configured {@link FailureHandler}.
     *
     * @param failureCtx Failure context.
     */
    public void process(FailureContext failureCtx) {
        process(failureCtx, hnd);
    }

    /**
     * Processes failure accordingly to given failure handler.
     *
     * @param failureCtx Failure context.
     * @param hnd Failure handler.
     */
    public synchronized void process(FailureContext failureCtx, FailureHandler hnd) {
        assert failureCtx != null;
        assert hnd != null;

        if (this.failureCtx != null) // Node already terminating, no reason to process more errors.
            return;

        U.error(ignite.log(), "Critical system error detected. Will be handled accordingly to configured handler " +
            "[hnd=" + hnd + ", failureCtx=" + failureCtx + ']', failureCtx.error());

        if (reserveBuf != null && X.hasCause(failureCtx.error(), OutOfMemoryError.class))
            reserveBuf = null;

        if (IGNITE_DUMP_THREADS_ON_FAILURE)
            U.dumpThreads(log);

        boolean invalidated = hnd.onFailure(ignite, failureCtx);

        if (invalidated) {
            this.failureCtx = failureCtx;

            log.error("Ignite node is in invalid state due to a critical failure.");
        }
    }
}
