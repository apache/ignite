/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dataload;

import org.gridgain.grid.*;
import org.gridgain.grid.dataload.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;

/**
 * Data loader future.
 */
class GridDataLoaderFuture extends GridFutureAdapter<Object> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Data loader. */
    @GridToStringExclude
    private IgniteDataLoader dataLdr;

    /**
     * Default constructor for {@link Externalizable} support.
     */
    public GridDataLoaderFuture() {
        // No-op.
    }

    /**
     * @param ctx Context.
     * @param dataLdr Data loader.
     */
    GridDataLoaderFuture(GridKernalContext ctx, IgniteDataLoader dataLdr) {
        super(ctx);

        assert dataLdr != null;

        this.dataLdr = dataLdr;
    }

    /** {@inheritDoc} */
    @Override public boolean cancel() throws GridException {
        checkValid();

        if (onCancelled()) {
            dataLdr.close(true);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDataLoaderFuture.class, this, super.toString());
    }
}
