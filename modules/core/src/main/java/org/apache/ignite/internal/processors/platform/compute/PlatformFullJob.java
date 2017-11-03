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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformProcessor;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper around job created in native platform.
 * <p>
 * If the job is expected to be executed locally, it contains only pointer to the corresponding entity in the native
 * platform. In case of topology change or failover, job is serialized on demand.
 * <p>
 * If we know in advance that the job is to be executed on remote node, then it is serialized into byte array right
 * away.
 * <p>
 * This class is not thread safe.
 */
@SuppressWarnings({"FieldCanBeLocal"})
public class PlatformFullJob extends PlatformAbstractJob {
    /** */
    private static final long serialVersionUID = 0L;

    /** Job is initialized. */
    private static final byte STATE_INIT = 0;

    /** Job is running. */
    private static final byte STATE_RUNNING = 1;

    /** Job execution completed. */
    private static final byte STATE_COMPLETED = 2;

    /** Job cancelled. */
    private static final byte STATE_CANCELLED = 3;

    /** Platform context. */
    private transient PlatformContext ctx;

    /** Serialized job. */
    private transient byte state;

    /**
     * {@link Externalizable} support.
     */
    @SuppressWarnings("UnusedDeclaration")
    public PlatformFullJob() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param ctx Platform context.
     * @param task Parent task.
     * @param ptr Job pointer.
     * @param job Job.
     */
    public PlatformFullJob(PlatformContext ctx, PlatformAbstractTask task, long ptr, Object job) {
        super(task, ptr, job);

        this.ctx = ctx;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object execute0(PlatformContext ctx) throws IgniteCheckedException {
        boolean cancel = false;

        synchronized (this) {
            // 1. Create job if necessary.
            if (task == null) {
                assert ptr == 0;

                createJob(ctx);
            }
            else
                assert ptr != 0;

            // 2. Set correct state.
            if (state == STATE_INIT)
                state = STATE_RUNNING;
            else {
                assert state == STATE_CANCELLED;

                cancel = true;
            }
        }

        try {
            if (task != null)
                return runLocal(ctx, cancel);
            else {
                try (PlatformMemory mem = ctx.memory().allocate()) {
                    PlatformOutputStream out = mem.output();

                    out.writeLong(ptr);
                    out.writeBoolean(cancel);  // cancel

                    out.synchronize();

                    ctx.gateway().computeJobExecute(mem.pointer());

                    PlatformInputStream in = mem.input();

                    in.synchronize();

                    BinaryRawReaderEx reader = ctx.reader(in);

                    return PlatformUtils.readInvocationResult(ctx, reader);
                }
            }
        }
        finally {
            synchronized (this) {
                if (task == null) {
                    assert ptr != 0;

                    ctx.gateway().computeJobDestroy(ptr);
                }

                if (state == STATE_RUNNING)
                    state = STATE_COMPLETED;
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        PlatformProcessor proc = PlatformUtils.platformProcessor(ignite);

        synchronized (this) {
            if (state == STATE_INIT)
                state = STATE_CANCELLED;
            else if (state == STATE_RUNNING) {
                assert ptr != 0;

                try {
                    proc.context().gateway().computeJobCancel(ptr);
                }
                finally {
                    state = STATE_CANCELLED;
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        if (job == null) {
            assert ptr != 0;

            try {
                if (task != null) {
                    if (task.onJobLock()) {
                        try {
                            serialize();
                        }
                        finally {
                            task.onJobUnlock();
                        }
                    }
                    else
                        throw new IgniteCheckedException("Task already completed: " + task);
                }
                else
                    serialize();
            }
            catch (IgniteCheckedException e) {
                throw new IOException("Failed to serialize interop job.", e);
            }
        }

        assert job != null;

        out.writeObject(job);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        job = in.readObject();
    }

    /**
     * Internal job serialization routine.
     *
     * @throws org.apache.ignite.IgniteCheckedException If failed.
     */
    private void serialize() throws IgniteCheckedException {
        try (PlatformMemory mem = ctx.memory().allocate()) {
            PlatformInputStream in = mem.input();

            boolean res = ctx.gateway().computeJobSerialize(ptr, mem.pointer()) == 1;

            in.synchronize();

            BinaryRawReaderEx reader = ctx.reader(in);

            if (res)
                job = reader.readObjectDetached();
            else
                throw new IgniteCheckedException(reader.readString());
        }
    }
}
