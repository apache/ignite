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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.internal.IgniteComputeImpl;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.binary.BinaryRawReaderEx;
import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.platform.PlatformAbstractTarget;
import org.apache.ignite.internal.processors.platform.PlatformContext;
import org.apache.ignite.internal.processors.platform.PlatformTarget;
import org.apache.ignite.internal.processors.platform.utils.PlatformFutureUtils;
import org.apache.ignite.internal.processors.platform.utils.PlatformListenable;
import org.apache.ignite.internal.processors.platform.utils.PlatformUtils;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteInClosure;

import static org.apache.ignite.internal.processors.task.GridTaskThreadContextKey.TC_SUBGRID;

/**
 * Interop compute.
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class PlatformCompute extends PlatformAbstractTarget {
    /** */
    private static final int OP_BROADCAST = 2;

    /** */
    private static final int OP_EXEC = 3;

    /** */
    private static final int OP_EXEC_ASYNC = 4;

    /** */
    private static final int OP_UNICAST = 5;

    /** */
    private static final int OP_WITH_NO_FAILOVER = 6;

    /** */
    private static final int OP_WITH_TIMEOUT = 7;

    /** */
    private static final int OP_EXEC_NATIVE = 8;

    /** */
    private static final int OP_WITH_NO_RESULT_CACHE = 9;

    /** */
    private static final int OP_WITH_EXECUTOR = 10;

    /** */
    private static final int OP_AFFINITY_CALL_PARTITION = 11;

    /** */
    private static final int OP_AFFINITY_RUN_PARTITION = 12;

    /** */
    private static final int OP_AFFINITY_CALL = 13;

    /** */
    private static final int OP_AFFINITY_RUN = 14;

    /** Compute instance. */
    private final IgniteComputeImpl compute;

    /** Compute instance for platform-only nodes. */
    private final IgniteComputeImpl computeForPlatform;

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param grp Cluster group.
     */
    public PlatformCompute(PlatformContext platformCtx, ClusterGroup grp, String platformAttr) {
        super(platformCtx);

        assert grp != null;
        assert platformAttr != null;

        compute = (IgniteComputeImpl)grp.ignite().compute(grp);

        ClusterGroup platformGrp = grp.forAttribute(platformAttr, platformCtx.platform());

        computeForPlatform = (IgniteComputeImpl)grp.ignite().compute(platformGrp);
    }

    /**
     * Constructor.
     *
     * @param platformCtx Context.
     * @param compute Compute.
     * @param computeForPlatform Compute over platform-specific nodes.
     */
    private PlatformCompute(PlatformContext platformCtx, IgniteComputeImpl compute,
                            IgniteComputeImpl computeForPlatform) {
        super(platformCtx);
        this.compute = compute;
        this.computeForPlatform = computeForPlatform;
    }

    /** {@inheritDoc} */
    @Override public PlatformTarget processInStreamOutObject(int type, BinaryRawReaderEx reader)
        throws IgniteCheckedException {
        switch (type) {
            case OP_UNICAST:
                return processClosures(reader.readLong(), reader, false);

            case OP_BROADCAST:
                return processClosures(reader.readLong(), reader, true);

            case OP_EXEC_NATIVE: {
                long taskPtr = reader.readLong();
                long topVer = reader.readLong();

                final PlatformFullTask task = new PlatformFullTask(platformCtx, computeForPlatform, taskPtr, topVer);

                return executeNative0(task);
            }

            case OP_EXEC_ASYNC:
                return wrapListenable((PlatformListenable) executeJavaTask(reader, true));

            case OP_WITH_EXECUTOR: {
                String executorName = reader.readString();

                return new PlatformCompute(platformCtx,
                        (IgniteComputeImpl)compute.withExecutor(executorName),
                        (IgniteComputeImpl)computeForPlatform.withExecutor(executorName));
            }

            case OP_AFFINITY_CALL_PARTITION: {
                Collection<String> cacheNames = PlatformUtils.readStrings(reader);
                int part = reader.readInt();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();

                PlatformCallable callable = new PlatformCallable(func, ptr);

                IgniteFuture future = compute.affinityCallAsync(cacheNames, part, callable);

                return wrapListenable(readAndListenFuture(reader, future));
            }

            case OP_AFFINITY_CALL: {
                String cacheName = reader.readString();
                Object key = reader.readObjectDetached();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();

                PlatformCallable callable = new PlatformCallable(func, ptr);

                IgniteFuture future = compute.affinityCallAsync(cacheName, key, callable);

                return wrapListenable(readAndListenFuture(reader, future));
            }

            case OP_AFFINITY_RUN_PARTITION: {
                Collection<String> cacheNames = PlatformUtils.readStrings(reader);
                int part = reader.readInt();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();

                PlatformRunnable runnable = new PlatformRunnable(func, ptr);

                IgniteFuture future = compute.affinityRunAsync(cacheNames, part, runnable);

                return wrapListenable(readAndListenFuture(reader, future));
            }

            case OP_AFFINITY_RUN: {
                String cacheName = reader.readString();
                Object key = reader.readObjectDetached();
                Object func = reader.readObjectDetached();
                long ptr = reader.readLong();

                PlatformRunnable runnable = new PlatformRunnable(func, ptr);

                IgniteFuture future = compute.affinityRunAsync(cacheName, key, runnable);

                return wrapListenable(readAndListenFuture(reader, future));
            }

            default:
                return super.processInStreamOutObject(type, reader);
        }
    }

    /** {@inheritDoc} */
    @Override public long processInLongOutLong(int type, long val) throws IgniteCheckedException {
        switch (type) {
            case OP_WITH_TIMEOUT: {
                compute.withTimeout(val);
                computeForPlatform.withTimeout(val);

                return TRUE;
            }

            case OP_WITH_NO_FAILOVER: {
                compute.withNoFailover();
                computeForPlatform.withNoFailover();

                return TRUE;
            }

            case OP_WITH_NO_RESULT_CACHE: {
                compute.withNoResultCache();
                computeForPlatform.withNoResultCache();

                return TRUE;
            }
        }

        return super.processInLongOutLong(type, val);
    }

    /**
     * Process closure execution request.
     *  @param taskPtr Task pointer.
     * @param reader Reader.
     * @param broadcast broadcast flag.
     */
    private PlatformTarget processClosures(long taskPtr, BinaryRawReaderEx reader, boolean broadcast) {
        PlatformAbstractTask task;

        int size = reader.readInt();

        if (size == 1) {
            if (broadcast) {
                PlatformBroadcastingSingleClosureTask task0 =
                    new PlatformBroadcastingSingleClosureTask(platformCtx, taskPtr);

                task0.job(nextClosureJob(task0, reader));

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

        platformCtx.kernalContext().task().setThreadContext(TC_SUBGRID, computeForPlatform.clusterGroup().nodes());

        return executeNative0(task);
    }

    /**
     * Read the next closure job from the reader.
     *
     * @param task Task.
     * @param reader Reader.
     * @return Closure job.
     */
    private PlatformJob nextClosureJob(PlatformAbstractTask task, BinaryRawReaderEx reader) {
        return platformCtx.createClosureJob(task, reader.readLong(), reader.readObjectDetached());
    }

    /** {@inheritDoc} */
    @Override public void processInStreamOutStream(int type, BinaryRawReaderEx reader, BinaryRawWriterEx writer)
        throws IgniteCheckedException {
        switch (type) {
            case OP_EXEC:
                writer.writeObjectDetached(executeJavaTask(reader, false));

                break;

            default:
                super.processInStreamOutStream(type, reader, writer);
        }
    }

    /**
     * Execute task.
     *
     * @param task Task.
     * @return Target.
     */
    private PlatformTarget executeNative0(final PlatformAbstractTask task) {
        IgniteInternalFuture fut = computeForPlatform.executeAsync0(task, null);

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

        return wrapListenable(PlatformFutureUtils.getListenable(fut));
    }

    /**
     * Execute task taking arguments from the given reader.
     *
     * @param reader Reader.
     * @param async Execute asynchronously flag.
     * @return Task result.
     * @throws IgniteCheckedException On error.
     */
    protected Object executeJavaTask(BinaryRawReaderEx reader, boolean async) throws IgniteCheckedException {
        String taskName = reader.readString();
        boolean keepBinary = reader.readBoolean();
        Object arg = reader.readObjectDetached();

        Collection<UUID> nodeIds = readNodeIds(reader);

        IgniteCompute compute0 = computeForTask(nodeIds);

        if (!keepBinary && arg instanceof BinaryObjectImpl)
            arg = ((BinaryObject)arg).deserialize();

        if (async)
            return readAndListenFuture(reader, new ComputeConvertingFuture(compute0.executeAsync(taskName, arg)));
        else
            return toBinary(compute0.execute(taskName, arg));
    }

    /**
     * Convert object to binary form.
     *
     * @param src Source object.
     * @return Result.
     */
    private Object toBinary(Object src) {
        return platformCtx.kernalContext().grid().binary().toBinary(src);
    }

    /**
     * Read node IDs.
     *
     * @param reader Reader.
     * @return Node IDs.
     */
    protected Collection<UUID> readNodeIds(BinaryRawReaderEx reader) {
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

    /**
     * Wraps ComputeTaskFuture as IgniteInternalFuture.
     */
    protected class ComputeConvertingFuture implements IgniteInternalFuture {
        /** */
        private final IgniteInternalFuture fut;

        /**
         * Ctor.
         *
         * @param fut Future to wrap.
         */
        public ComputeConvertingFuture(ComputeTaskFuture fut) {
            this.fut = ((IgniteFutureImpl)fut).internalFuture();
        }

        /** {@inheritDoc} */
        @Override public Object get() throws IgniteCheckedException {
            return convertResult(fut.get());
        }

        /** {@inheritDoc} */
        @Override public Object get(long timeout) throws IgniteCheckedException {
            return convertResult(fut.get(timeout));
        }

        /** {@inheritDoc} */
        @Override public Object get(long timeout, TimeUnit unit) throws IgniteCheckedException {
            return convertResult(fut.get(timeout, unit));
        }

        /** {@inheritDoc} */
        @Override public Object getUninterruptibly() throws IgniteCheckedException {
            return convertResult(fut.get());
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            return fut.cancel();
        }

        /** {@inheritDoc} */
        @Override public boolean isDone() {
            return fut.isDone();
        }

        /** {@inheritDoc} */
        @Override public boolean isCancelled() {
            return fut.isCancelled();
        }

        /** {@inheritDoc} */
        @Override public void listen(final IgniteInClosure lsnr) {
            fut.listen(new IgniteInClosure<IgniteInternalFuture>() {
                private static final long serialVersionUID = 0L;

                @Override public void apply(IgniteInternalFuture fut0) {
                    lsnr.apply(ComputeConvertingFuture.this);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chain(IgniteClosure doneCb) {
            throw new UnsupportedOperationException("Chain operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public IgniteInternalFuture chain(IgniteClosure doneCb, Executor exec) {
            throw new UnsupportedOperationException("Chain operation is not supported.");
        }

        /** {@inheritDoc} */
        @Override public Throwable error() {
            return fut.error();
        }

        /** {@inheritDoc} */
        @Override public Object result() {
            return convertResult(fut.result());
        }

        /**
         * Converts future result.
         *
         * @param obj Object to convert.
         * @return Result.
         */
        protected Object convertResult(Object obj) {
            return toBinary(obj);
        }
    }
}
