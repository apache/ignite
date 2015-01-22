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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.job.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Remote job context implementation.
 */
public class GridJobContextImpl implements ComputeJobContext, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Kernal context ({@code null} for job result context). */
    private GridKernalContext ctx;

    /** */
    private IgniteUuid jobId;

    /** Job worker. */
    private GridJobWorker job;

    /** Logger. */
    private IgniteLogger log;

    /** Attributes mux. Do not use this as object is exposed to user. */
    private final Object mux = new Object();

    /** */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    @GridToStringInclude
    private Map<Object, Object> attrs;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridJobContextImpl() {
        // No-op.
    }

    /**
     * @param ctx Kernal context.
     * @param jobId Job ID.
     */
    public GridJobContextImpl(@Nullable GridKernalContext ctx, IgniteUuid jobId) {
        assert jobId != null;

        this.ctx = ctx;
        this.jobId = jobId;
        attrs = U.newHashMap(1);
    }

    /**
     * @param ctx Kernal context.
     * @param jobId Job ID.
     * @param attrs Job attributes.
     */
    public GridJobContextImpl(GridKernalContext ctx, IgniteUuid jobId,
        Map<? extends Serializable, ? extends Serializable> attrs) {
        this(ctx, jobId);

        synchronized (mux) {
            this.attrs.putAll(attrs);
        }
    }

    /**
     * @param job Job worker.
     */
    public void job(GridJobWorker job) {
        assert job != null;

        this.job = job;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getJobId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, @Nullable Object val) {
        A.notNull(key, "key");

        synchronized (mux) {
            attrs.put(key, val);
        }
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) {
        A.notNull(attrs, "attrs");

        synchronized (mux) {
            this.attrs.putAll(attrs);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> V getAttribute(K key) {
        A.notNull(key, "key");

        synchronized (mux) {
            return (V)attrs.get(key);
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> getAttributes() {
        synchronized (mux) {
            return attrs.isEmpty() ? Collections.emptyMap() : U.sealMap(attrs);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean heldcc() {
        if (ctx == null)
            return false;

        if (job == null)
            job = ctx.job().activeJob(jobId);

        return job != null && job.held();
    }

    /** {@inheritDoc} */
    @SuppressWarnings( {"RedundantTypeArguments"})
    @Override public <T> T holdcc() {
        return this.<T>holdcc(0);
    }

    /** {@inheritDoc} */
    @Override public <T> T holdcc(long timeout) {
        if (ctx != null) {
            if (job == null)
                job = ctx.job().activeJob(jobId);

            // Completed?
            if (job != null) {
                job.hold();

                if (timeout > 0 && !job.isDone()) {
                    final long endTime = U.currentTimeMillis() + timeout;

                    // Overflow.
                    if (endTime > 0) {
                        ctx.timeout().addTimeoutObject(new GridTimeoutObject() {
                            private final IgniteUuid id = IgniteUuid.randomUuid();

                            @Override public IgniteUuid timeoutId() {
                                return id;
                            }

                            @Override public long endTime() {
                                return endTime;
                            }

                            @Override public void onTimeout() {
                                try {
                                    ExecutorService execSvc = job.isInternal() ?
                                        ctx.config().getManagementExecutorService() : ctx.config().getExecutorService();

                                    assert execSvc != null;

                                    execSvc.submit(new Runnable() {
                                        @Override public void run() {
                                            callcc();
                                        }
                                    });
                                }
                                catch (RejectedExecutionException e) {
                                    U.error(log(), "Failed to execute job (will execute synchronously).", e);

                                    callcc();
                                }
                            }
                        });
                    }
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void callcc() {
        if (ctx != null) {
            if (job == null)
                job = ctx.job().activeJob(jobId);

            if (job != null)
                // Execute in the same thread.
                job.execute();
        }
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        try {
            return (String)job.getDeployment().annotatedValue(job.getJob(), GridCacheName.class);
        }
        catch (IgniteCheckedException e) {
            throw F.wrap(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T affinityKey() {
        try {
            return (T)job.getDeployment().annotatedValue(job.getJob(), GridCacheAffinityKeyMapped.class);
        }
        catch (IgniteCheckedException e) {
            throw F.wrap(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(ctx);
        U.writeGridUuid(out, jobId);
        U.writeMap(out, getAttributes());
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        ctx = (GridKernalContext)in.readObject();
        jobId = U.readGridUuid(in);
        attrs = U.readMap(in);
    }

    /**
     * @return Logger.
     */
    private IgniteLogger log() {
        assert ctx != null;

        if (log == null)
            log = U.logger(ctx, logRef, GridJobContextImpl.class);

        return log;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridJobContextImpl.class, this);
    }
}
