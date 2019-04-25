/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.thread;

import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.X;

/**
 * OOM exception handler for system threads.
 */
public class OomExceptionHandler implements Thread.UncaughtExceptionHandler {
    /** Context. */
    private final GridKernalContext ctx;

    /**
     * @param ctx Context.
     */
    public OomExceptionHandler(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void uncaughtException(Thread t, Throwable e) {
        if (X.hasCause(e, OutOfMemoryError.class))
            ctx.failure().process(new FailureContext(FailureType.CRITICAL_ERROR, e));
    }
}
