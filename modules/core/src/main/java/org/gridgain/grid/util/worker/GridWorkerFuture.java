/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.worker;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.future.*;

import java.io.*;

/**
 * Future for locally executed closure that defines cancellation logic.
 */
public class GridWorkerFuture<T> extends GridFutureAdapter<T> {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private GridWorker w;

    /**
     * @param ctx Context.
     */
    public GridWorkerFuture(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridWorkerFuture() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws IgniteCheckedException {
        assert w != null;

        if (!onCancelled())
            return false;

        w.cancel();

        return true;
    }

    /**
     * @param w Worker.
     */
    public void setWorker(GridWorker w) {
        assert w != null;

        this.w = w;
    }
}
