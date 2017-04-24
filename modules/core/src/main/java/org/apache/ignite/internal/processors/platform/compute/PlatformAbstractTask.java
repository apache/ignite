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

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformNativeException;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.typedef.X;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for all interop tasks.
 */
public abstract class PlatformAbstractTask implements ComputeTask<Object, Void> {
    /** Platform context. */
    protected final PlatformContext ctx;

    /** Pointer to the task in the native platform. */
    protected final long taskPtr;

    /** Lock for safe access to native pointers. */
    protected final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Done flag. */
    protected boolean done;

    /**
     * Constructor.
     *
     * @param ctx Platform context.
     * @param taskPtr Task pointer.
     */
    protected PlatformAbstractTask(PlatformContext ctx, long taskPtr) {
        this.ctx = ctx;
        this.taskPtr = taskPtr;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ThrowableResultOfMethodCallIgnored", "unchecked"})
    @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
        assert rcvd.isEmpty() : "Should not cache result in Java for interop task";

        lock.readLock().lock();

        try {
            assert !done;

            PlatformAbstractJob job = res.getJob();

            assert job.pointer() != 0;

            Object res0bj = res.getData();

            int plc;

            if (res0bj == PlatformAbstractJob.LOC_JOB_RES)
                // Processing local job execution result.
                plc = ctx.gateway().computeTaskLocalJobResult(taskPtr, job.pointer());
            else {
                // Processing remote job execution result or exception.
                try (PlatformMemory mem = ctx.memory().allocate()) {
                    PlatformOutputStream out = mem.output();

                    BinaryRawWriterEx writer = ctx.writer(out);

                    writer.writeLong(taskPtr);
                    writer.writeLong(job.pointer());

                    writer.writeUuid(res.getNode().id());
                    writer.writeBoolean(res.isCancelled());

                    IgniteException err = res.getException();

                    PlatformUtils.writeInvocationResult(writer, res0bj, err);

                    out.synchronize();

                    plc = ctx.gateway().computeTaskJobResult(mem.pointer());
                }
            }

            ComputeJobResultPolicy plc0 = ComputeJobResultPolicy.fromOrdinal((byte) plc);

            assert plc0 != null : plc;

            return plc0;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
        assert results.isEmpty() : "Should not cache result in java for interop task";

        lock.readLock().lock();

        try {
            assert !done;

            ctx.gateway().computeTaskReduce(taskPtr);
        }
        finally {
            lock.readLock().unlock();
        }

        return null;
    }

    /**
     * Callback invoked when task future is completed and all resources could be safely cleaned up.
     *
     * @param e If failed.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void onDone(Exception e) {
        lock.writeLock().lock();

        try {
            assert !done;

            if (e == null)
                // Normal completion.
                ctx.gateway().computeTaskComplete(taskPtr, 0);
            else {
                PlatformNativeException e0 = X.cause(e, PlatformNativeException.class);

                try (PlatformMemory mem = ctx.memory().allocate()) {
                    PlatformOutputStream out = mem.output();

                    BinaryRawWriterEx writer = ctx.writer(out);

                    if (e0 == null) {
                        writer.writeBoolean(false);
                        writer.writeString(e.getClass().getName());
                        writer.writeString(e.getMessage());
                        writer.writeString(X.getFullStackTrace(e));
                    }
                    else {
                        writer.writeBoolean(true);
                        writer.writeObject(e0.cause());
                    }

                    out.synchronize();

                    ctx.gateway().computeTaskComplete(taskPtr, mem.pointer());
                }
            }
        }
        finally {
            // Done flag is set irrespective of any exceptions.
            done = true;

            lock.writeLock().unlock();
        }
    }

    /**
     * Callback invoked by job when it wants to lock the task.
     *
     * @return {@code} True if task is not completed yet, {@code false} otherwise.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    boolean onJobLock() {
        lock.readLock().lock();

        if (done) {
            lock.readLock().unlock();

            return false;
        }
        else
            return true;
    }

    /**
     * Callback invoked by job when task can be unlocked.
     */
    void onJobUnlock() {
        assert !done;

        lock.readLock().unlock();
    }
}
