/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.security.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;

/**
 * This class provide implementation for task future.
 * @param <R> Type of the task result returning from {@link GridComputeTask#reduce(List)} method.
 */
public class GridTaskFutureImpl<R> extends GridFutureAdapter<R> implements GridComputeTaskFuture<R> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridComputeTaskSession ses;

    /** Mapped flag. */
    private volatile boolean mapped;

    /** Mapped latch. */
    @GridToStringExclude
    private CountDownLatch mappedLatch = new CountDownLatch(1);

    /** */
    private GridKernalContext ctx;

    /**
     * Required by {@link Externalizable}.
     */
    public GridTaskFutureImpl() {
        // No-op.
    }

    /**
     * @param ses Task session instance.
     * @param ctx Kernal context.
     */
    public GridTaskFutureImpl(GridComputeTaskSession ses, GridKernalContext ctx) {
        super(ctx);

        assert ses != null;
        assert ctx != null;

        this.ses = ses;
        this.ctx = ctx;
    }

    /**
     * Gets task timeout.
     *
     * @return Task timeout.
     */
    @Override public GridComputeTaskSession getTaskSession() {
        if (ses == null)
            throw new IllegalStateException("Cannot access task session after future has been deserialized.");

        return ses;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws GridException {
        ctx.security().authorize(ses.getTaskName(), GridSecurityPermission.TASK_CANCEL, null);

        checkValid();

        if (onCancelled()) {
            ctx.task().onCancelled(ses.getId());

            return true;
        }

        return isCancelled();
    }

    /**
     * Cancel task on master leave event. Does not send cancel request to remote jobs and invokes master-leave
     * callback on local jobs.
     *
     * @return {@code True} if future was cancelled (i.e. was not finished prior to this call).
     * @throws GridException If failed.
     */
    public boolean cancelOnMasterLeave() throws GridException {
        checkValid();

        if (onCancelled()) {
            // Invoke master-leave callback on spawned jobs on local node and then cancel them.
            for (GridNode node : ctx.discovery().nodes(ses.getTopology())) {
                if (ctx.localNodeId().equals(node.id())) {
                    ctx.job().masterLeaveLocal(ses.getId());

                    ctx.job().cancelJob(ses.getId(), null, false);
                }
            }

            return true;
        }

        return isCancelled();
    }

    /** {@inheritDoc} */
    @Override public boolean isMapped() {
        return mapped;
    }

    /**
     * Callback for completion of task mapping stage.
     */
    public void onMapped() {
        mapped = true;

        mappedLatch.countDown();
    }

    /** {@inheritDoc} */
    @Override public boolean onDone(@Nullable R res, @Nullable Throwable err) {
        if (super.onDone(res, err)) {
            if (mappedLatch.getCount() > 0)
                mappedLatch.countDown();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean onCancelled() {
        if (super.onCancelled()) {
            if (mappedLatch.getCount() > 0)
                mappedLatch.countDown();

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean waitForMap(long timeout) throws GridException {
        if (!mapped && !isDone()) {
            if (timeout == 0)
                U.await(mappedLatch);
            else
                U.await(mappedLatch, timeout, MILLISECONDS);
        }

        return mapped;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeBoolean(mapped);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        mapped = in.readBoolean();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskFutureImpl.class, this, "mappedLatchCnt", mappedLatch.getCount(),
            "super", super.toString());
    }
}
