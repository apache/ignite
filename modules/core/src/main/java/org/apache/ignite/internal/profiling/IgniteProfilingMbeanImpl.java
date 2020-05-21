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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;

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
        ctx.closure().broadcast(new ProfilingJob(), true, ctx.discovery().allNodes(), null).get();
    }

    /** {@inheritDoc} */
    @Override public void stopProfiling() throws IgniteCheckedException {
        ctx.closure().broadcast(new ProfilingJob(), false, ctx.discovery().allNodes(), null).get();
    }

    /** {@inheritDoc} */
    @Override public boolean profilingEnabled() {
        return ctx.metric().profilingEnabled();
    }

    /** Job to start/stop profiling. */
    @GridInternal
    private static class ProfilingJob implements IgniteClosure<Boolean, Void> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private transient IgniteEx ignite;

        /** @param start {@code True} if start profiling. {@code True} if stop profiling. */
        @Override public Void apply(Boolean start) {
            if (start) {
                ignite.context().metric().startProfiling(LogFileProfiling.DFLT_FILE_MAX_SIZE,
                    LogFileProfiling.DFLT_BUFFER_SIZE, LogFileProfiling.DFLT_FLUSH_SIZE);
            }
            else
                ignite.context().metric().stopProfiling();

            return null;
        }
    }
}
