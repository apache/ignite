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

package org.apache.ignite.internal.processors.platform.compute;

import java.io.Externalizable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.jetbrains.annotations.Nullable;

/**
 * Base interop job.
 */
public abstract class PlatformAbstractJob implements PlatformJob, Externalizable {
    /** Marker object denoting the job execution result is stored in native platform. */
    static final Object LOC_JOB_RES = new Object();

    /** Grid name. */
    @IgniteInstanceResource
    protected transient Ignite ignite;

    /** Parent task; present only on local job instance. */
    protected transient PlatformAbstractTask task;

    /** Pointer to job in the native platform. */
    protected transient long ptr;

    /** Job. */
    protected Object job;

    /**
     * {@link java.io.Externalizable} support.
     */
    protected PlatformAbstractJob() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param task Parent task.
     * @param ptr Pointer.
     * @param job Job.
     */
    protected PlatformAbstractJob(PlatformAbstractTask task, long ptr, Object job) {
        this.task = task;
        this.ptr = ptr;
        this.job = job;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object execute() {
        try {
            PlatformProcessor interopProc = PlatformUtils.platformProcessor(ignite);

            interopProc.awaitStart();

            return execute0(interopProc.context());
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /**
     * Internal job execution routine.
     *
     * @param ctx Interop processor.
     * @return Result.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    protected abstract Object execute0(PlatformContext ctx) throws IgniteCheckedException;

    /**
     * Create job in native platform if needed.
     *
     * @param ctx Context.
     * @return {@code True} if job was created, {@code false} if this is local job and creation is not necessary.
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    protected boolean createJob(PlatformContext ctx) throws IgniteCheckedException {
        if (ptr == 0) {
            try (PlatformMemory mem = ctx.memory().allocate()) {
                PlatformOutputStream out = mem.output();

                BinaryRawWriterEx writer = ctx.writer(out);

                writer.writeObject(job);

                out.synchronize();

                ptr = ctx.gateway().computeJobCreate(mem.pointer());
            }

            return true;
        }
        else
            return false;
    }

    /**
     * Run local job.
     *
     * @param ctx Context.
     * @param cancel Cancel flag.
     * @return Result.
     */
    protected Object runLocal(PlatformContext ctx, boolean cancel) {
        // Local job, must execute it with respect to possible concurrent task completion.
        if (task.onJobLock()) {
            try {
                ctx.gateway().computeJobExecuteLocal(ptr, cancel ? 1 : 0);

                return LOC_JOB_RES;
            }
            finally {
                task.onJobUnlock();
            }
        }
        else
            // Task has completed concurrently, no need to run the job.
            return null;
    }

    /** {@inheritDoc} */
    @Override public long pointer() {
        return ptr;
    }

    /** {@inheritDoc} */
    @Override public Object job() {
        return job;
    }
}
