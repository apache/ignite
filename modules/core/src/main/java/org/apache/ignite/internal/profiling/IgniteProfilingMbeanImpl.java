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

package org.apache.ignite.internal.profiling;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.A;

import static org.apache.ignite.internal.profiling.LogFileProfiling.DFLT_BUFFER_SIZE;
import static org.apache.ignite.internal.profiling.LogFileProfiling.DFLT_FILE_MAX_SIZE;
import static org.apache.ignite.internal.profiling.LogFileProfiling.DFLT_FLUSH_SIZE;

/**
 * {@link IgniteProfilingMBean} implementation.
 */
public class IgniteProfilingMbeanImpl implements IgniteProfilingMBean {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** @param ctx Kernal context. */
    public IgniteProfilingMbeanImpl(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Override public void startProfiling() throws IgniteCheckedException {
        ctx.metric().startProfiling(DFLT_FILE_MAX_SIZE, DFLT_BUFFER_SIZE, DFLT_FLUSH_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void startProfiling(long maxFileSize, int bufferSize, int flushBatchSize)
        throws IgniteCheckedException {
        A.ensure(maxFileSize > 0, "maxFileSize > 0");
        A.ensure(bufferSize > 0, "bufferSize > 0");
        A.ensure(flushBatchSize >= 0, "flushBatchSize >= 0");

        ctx.metric().startProfiling(maxFileSize, bufferSize, flushBatchSize);
    }

    /** {@inheritDoc} */
    @Override public void stopProfiling() throws IgniteCheckedException {
        ctx.metric().stopProfiling().get();
    }

    /** {@inheritDoc} */
    @Override public boolean profilingEnabled() {
        return ctx.metric().profilingEnabled();
    }
}
