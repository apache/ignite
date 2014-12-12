/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.kernal.processors.job.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Remote job context implementation.
 */
public class GridJobContextImpl extends GridMetadataAwareAdapter implements ComputeJobContext, Externalizable {
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
