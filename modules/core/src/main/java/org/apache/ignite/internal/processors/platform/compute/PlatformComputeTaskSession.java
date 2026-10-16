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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.internal.binary.BinaryReaderEx;
import org.apache.ignite.internal.binary.BinaryWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.memory.PlatformMemory;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.platform.utils.PlatformUtils.readMap;

/** {@link ComputeTaskSession} platform wrapper. */
public class PlatformComputeTaskSession extends PlatformAbstractTarget {
    /** "get attribute" operation code. */
    private static final int OP_GET_ATTRIBUTE = 1;

    /** "set attributes" operation code. */
    private static final int OP_SET_ATTRIBUTES = 2;

    /** "send job to specific node" operation code. */
    private static final int OP_SEND_JOB = 3;

    /** "send job to random node" operation code. */
    private static final int OP_SEND_JOB_RANDOM = 4;

    /** Underlying compute task session. */
    private final ComputeTaskSession ses;

    /** This session's task. */
    private final PlatformAbstractTask task;

    /** This session's task name. */
    private final String taskName;

    /** Underlying compute task mapper. */
    private final ComputeTaskContinuousMapper mapper;

    /**
     * Constructor for compute task session.
     *
     * @param platformCtx Context.
     * @param ses         Underlying compute task session
     * @param task        This session's task
     * @param taskName    This session's task name
     * @param mapper      Underlying compute task mapper
     */
    public PlatformComputeTaskSession(
        final PlatformContext platformCtx,
        final ComputeTaskSession ses,
        final PlatformAbstractTask task,
        final String taskName,
        final ComputeTaskContinuousMapper mapper) {
        super(platformCtx);

        this.ses = ses;
        this.task = task;
        this.taskName = taskName;
        this.mapper = mapper;
    }

    /**
     * Constructor for compute job session.
     *
     * @param platformCtx Context.
     * @param ses         Underlying compute task session
     */
    public PlatformComputeTaskSession(final PlatformContext platformCtx, final ComputeTaskSession ses) {
        this(platformCtx, ses, null, null, null);
    }

    /** {@inheritDoc} */
    @Override public long processInStreamOutLong(
        final int type, final BinaryReaderEx reader, final PlatformMemory mem) throws IgniteCheckedException {

        switch (type) {
            case OP_SET_ATTRIBUTES:
                final Map<?, ?> attrs = readMap(reader);

                ses.setAttributes(attrs);

                return TRUE;

            case OP_SEND_JOB: {
                final int size = reader.readInt();
                final Map<ComputeJob, ClusterNode> jobs = U.newHashMap(size);

                for (var i = 0; i < size; i++) {
                    final long ptr = reader.readLong();
                    final Object nativeJob = reader.readBoolean() ? reader.readObjectDetached() : null;
                    final PlatformJob job = platformCtx.createJob(task, ptr, nativeJob, taskName);
                    final UUID nodeId = reader.readUuid();
                    final ClusterNode node = platformCtx.kernalContext().discovery().node(nodeId);

                    jobs.put(job, node);
                }

                mapper.send(jobs);

                return TRUE;
            }

            case OP_SEND_JOB_RANDOM: {
                final int size = reader.readInt();
                final Collection<ComputeJob> jobs = new ArrayList<>(size);

                for (var i = 0; i < size; i++) {
                    final long ptr = reader.readLong();
                    final Object nativeJob = reader.readObjectDetached();
                    final PlatformJob job = platformCtx.createJob(task, ptr, nativeJob, taskName);

                    jobs.add(job);
                }

                mapper.send(jobs);

                return TRUE;
            }

            default:
                return super.processInStreamOutLong(type, reader, mem);
        }
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(
        final int type, final BinaryReaderEx reader, final BinaryWriterEx writer) throws IgniteCheckedException {

        if (type == OP_GET_ATTRIBUTE) {
            final Object key = reader.readObjectDetached();

            final Object val = ses.getAttribute(key);

            writer.writeObjectDetached(val);

            return;
        }

        super.processInStreamOutStream(type, reader, writer);
    }
}
