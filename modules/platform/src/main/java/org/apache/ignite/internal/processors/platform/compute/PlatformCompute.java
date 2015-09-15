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
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.internal.IgniteComputeImpl;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.portable.PortableObjectImpl;
import org.apache.ignite.internal.portable.PortableRawReaderEx;
import org.apache.ignite.internal.portable.PortableRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.internal.portable.api.PortableObject;

import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID;

/**
 * Interop compute.
 */
@SuppressWarnings({"unchecked", "ThrowableResultOfMethodCallIgnored", "UnusedDeclaration"})
public class PlatformCompute extends PlatformAbstractTarget {
    /** */
    private static final int OP_AFFINITY = 1;

    /** */
    private static final int OP_BROADCAST = 2;

    /** */
    private static final int OP_EXEC = 3;

    /** */
    private static final int OP_EXEC_ASYNC = 4;

    /** */
    private static final int OP_UNICAST = 5;

    /** Compute instance. */
    private final IgniteComputeImpl compute;

    /** Future for previous asynchronous operation. */
    protected ThreadLocal<IgniteFuture<?>> curFut = new ThreadLocal<>();
    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param compute Compute instance.
     */
    public PlatformCompute(PlatformContext platformCtx, IgniteComputeImpl compute) {
        super(platformCtx);

        this.compute = compute;
    }

    /** {@inheritDoc} */
    @Override protected long processInStreamOutLong(int type, PortableRawReaderEx reader) throws IgniteCheckedException {
        switch (type) {
            case OP_UNICAST:
                processClosures(reader.readLong(), reader, false, false);

                return TRUE;

            case OP_BROADCAST:
                processClosures(reader.readLong(), reader, true, false);

                return TRUE;

            case OP_AFFINITY:
                processClosures(reader.readLong(), reader, false, true);

                return TRUE;

            default:
                return super.processInStreamOutLong(type, reader);
        }
    }

    /**
     * Process closure execution request.
     *
     * @param taskPtr Task pointer.
     * @param reader Reader.
     * @param broadcast broadcast flag.
     */
    private void processClosures(long taskPtr, PortableRawReaderEx reader, boolean broadcast, boolean affinity) {
        PlatformAbstractTask task;

        int size = reader.readInt();

        if (size == 1) {
            if (broadcast) {
                PlatformBroadcastingSingleClosureTask task0 =
                    new PlatformBroadcastingSingleClosureTask(platformCtx, taskPtr);

                task0.job(nextClosureJob(task0, reader));

                task = task0;
            }
            else if (affinity) {
                PlatformBalancingSingleClosureAffinityTask task0 =
                    new PlatformBalancingSingleClosureAffinityTask(platformCtx, taskPtr);

                task0.job(nextClosureJob(task0, reader));

                task0.affinity(reader.readString(), reader.readObjectDetached(), platformCtx.kernalContext());

                task = task0;
            }
            else {
                PlatformBalancingSingleClosureTask task0 = new PlatformBalancingSingleClosureTask(platformCtx, taskPtr);

                task0.job(nextClosureJob(task0, reader));

                task = task0;
            }
        }
        else {
            if (broadcast)
                task = new PlatformBroadcastingMultiClosureTask(platformCtx, taskPtr);
            else
                task = new PlatformBalancingMultiClosureTask(platformCtx, taskPtr);

            Collection<PlatformJob> jobs = new ArrayList<>(size);

            for (int i = 0; i < size; i++)
                jobs.add(nextClosureJob(task, reader));

            if (broadcast)
                ((PlatformBroadcastingMultiClosureTask)task).jobs(jobs);
            else
                ((PlatformBalancingMultiClosureTask)task).jobs(jobs);
        }

        platformCtx.kernalContext().task().setThreadContext(TC_SUBGRID, compute.clusterGroup().nodes());

        executeNative0(task);
    }

    /**
     * Read the next closure job from the reader.
     *
     * @param task Task.
     * @param reader Reader.
     * @return Closure job.
     */
    private PlatformJob nextClosureJob(PlatformAbstractTask task, PortableRawReaderEx reader) {
        return platformCtx.createClosureJob(task, reader.readLong(), reader.readObjectDetached());
    }

    /** {@inheritDoc} */
    @Override protected void processInStreamOutStream(int type, PortableRawReaderEx reader, PortableRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_EXEC:
                writer.writeObjectDetached(executeJavaTask(reader, false));

                break;

            case OP_EXEC_ASYNC:
                writer.writeObjectDetached(executeJavaTask(reader, true));

                break;

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /**
     * Execute native full-fledged task.
     *
     * @param taskPtr Pointer to the task.
     * @param topVer Topology version.
     */
    public void executeNative(long taskPtr, long topVer) {
        final PlatformFullTask task = new PlatformFullTask(platformCtx, compute, taskPtr, topVer);

        executeNative0(task);
    }

    /**
     * Set "withTimeout" state.
     *
     * @param timeout Timeout (milliseconds).
     */
    public void withTimeout(long timeout) {
        compute.withTimeout(timeout);
    }

    /**
     * Set "withNoFailover" state.
     */
    public void withNoFailover() {
        compute.withNoFailover();
    }

    /** <inheritDoc /> */
    @Override protected IgniteFuture currentFuture() throws IgniteCheckedException {
        IgniteFuture<?> fut = curFut.get();

        if (fut == null)
            throw new IllegalStateException("Asynchronous operation not started.");

        return fut;
    }

    /**
     * Execute task.
     *
     * @param task Task.
     */
    private void executeNative0(final PlatformAbstractTask task) {
        IgniteInternalFuture fut = compute.executeAsync(task, null);

        fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
            private static final long serialVersionUID = 0L;

            @Override public void apply(IgniteInternalFuture fut) {
                try {
                    fut.get();

                    task.onDone(null);
                }
                catch (IgniteCheckedException e) {
                    task.onDone(e);
                }
            }
        });
    }

    /**
     * Execute task taking arguments from the given reader.
     *
     * @param reader Reader.
     * @return Task result.
     */
    protected Object executeJavaTask(PortableRawReaderEx reader, boolean async) {
        String taskName = reader.readString();
        boolean keepPortable = reader.readBoolean();
        Object arg = reader.readObjectDetached();

        Collection<UUID> nodeIds = readNodeIds(reader);

        IgniteCompute compute0 = computeForTask(nodeIds);

        if (async)
            compute0 = compute0.withAsync();

        if (!keepPortable && arg instanceof PortableObjectImpl)
            arg = ((PortableObject)arg).deserialize();

        Object res = compute0.execute(taskName, arg);

        if (async) {
            curFut.set(compute0.future().chain(new C1<IgniteFuture, Object>() {
                private static final long serialVersionUID = 0L;

                @Override public Object apply(IgniteFuture fut) {
                    return toPortable(fut.get());
                }
            }));

            return null;
        }
        else
            return toPortable(res);
    }

    /**
     * Convert object to portable form.
     *
     * @param src Source object.
     * @return Result.
     */
    private Object toPortable(Object src) {
        return platformCtx.kernalContext().grid().portables().toPortable(src);
    }

    /**
     * Read node IDs.
     *
     * @param reader Reader.
     * @return Node IDs.
     */
    protected Collection<UUID> readNodeIds(PortableRawReaderEx reader) {
        if (reader.readBoolean()) {
            int len = reader.readInt();

            List<UUID> res = new ArrayList<>(len);

            for (int i = 0; i < len; i++)
                res.add(reader.readUuid());

            return res;
        }
        else
            return null;
    }

    /**
     * Get compute object for the given node IDs.
     *
     * @param nodeIds Node IDs.
     * @return Compute object.
     */
    protected IgniteCompute computeForTask(Collection<UUID> nodeIds) {
        return nodeIds == null ? compute :
            platformCtx.kernalContext().grid().compute(compute.clusterGroup().forNodeIds(nodeIds));
    }
}