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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * General failure processing API
 */
public class FailureProcessor extends GridProcessorAdapter {
    /** Ignite. */
    private final Ignite ignite;

    /** Handler. */
    private volatile FailureHandler hnd;

    /** Failure context. */
    private volatile FailureContext failureCtx;

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

        A.notNull(hnd, "Either configured or default failure handler can't be null");

        this.hnd = hnd;
    }

    /**
     * This method is used to initialize local failure handler if {@link IgniteConfiguration} don't contain configured one.
     *
     * @return Default {@link FailureHandler} implementation.
     */
    protected FailureHandler getDefaultFailureHandler() {
        return new StopNodeOrHaltFailureHandler(false, 0);
    }

    /**
     * @return Failure handler.
     */
    private FailureHandler handler() {
        FailureHandler hnd = this.hnd;

        if (hnd == null)
            hnd = getDefaultFailureHandler();

        return hnd;
    }

    /**
     * @return Failure context.
     */
    public FailureContext failureContext() {
        return failureCtx;
    }

    /**
     * Processes some failure.
     * May cause node termination.
     *
     * @param failureCtx Failure context.
     */
    public void process(FailureContext failureCtx) {
        handleFailure(failureCtx, handler());
    }

    /**
     * @param failureCtx Failure context.
     * @param hnd Handler.
     */
    public synchronized void handleFailure(FailureContext failureCtx, FailureHandler hnd) {
        assert failureCtx != null;
        assert hnd != null;

        if (this.failureCtx != null) // Node already terminating, no reason to process more errors.
            return;

        if (hnd.onFailure(ignite, failureCtx))
            this.failureCtx = failureCtx;
    }
}
