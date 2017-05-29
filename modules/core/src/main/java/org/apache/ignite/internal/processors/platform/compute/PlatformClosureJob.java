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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformInputStream;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.processors.platform.memory.PlatformOutputStream;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Light-weight interop job. Comparing to regular job, this guy has simpler logic because we should not
 * bother with delayed serialization and cancellation.
 */
public class PlatformClosureJob extends PlatformAbstractJob {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * {@link java.io.Externalizable} support.
     */
    public PlatformClosureJob() {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param task Parent task.
     * @param ptr Job pointer.
     * @param job Job.
     */
    public PlatformClosureJob(PlatformAbstractTask task, long ptr, Object job) {
        super(task, ptr, job);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Object execute0(PlatformContext ctx) throws IgniteCheckedException {
        if (task == null) {
            // Remote job execution.
            assert ptr == 0;

            createJob(ctx);

            try (PlatformMemory mem = ctx.memory().allocate()) {
                PlatformOutputStream out = mem.output();

                out.writeLong(ptr);
                out.writeBoolean(false);  // cancel

                out.synchronize();

                ctx.gateway().computeJobExecute(mem.pointer());

                PlatformInputStream in = mem.input();

                in.synchronize();

                BinaryRawReaderEx reader = ctx.reader(in);

                return PlatformUtils.readInvocationResult(ctx, reader);
            }
            finally {
                ctx.gateway().computeJobDestroy(ptr);
            }
        }
        else {
            // Local job execution.
            assert ptr != 0;

            return runLocal(ctx, false);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        assert job != null;

        out.writeObject(job);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        job = in.readObject();
    }
}
