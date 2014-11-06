/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.interop.os;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.interop.*;
import org.jetbrains.annotations.*;

/**
 * OS interop processor.
 */
public class GridOsInteropProcessor extends GridInteropProcessorAdapter {
    /** Common error message. */
    private static final String ERR_MSG = "Interop feature is not supported in OS edition.";

    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    public GridOsInteropProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void releaseStart() {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public void awaitStart() throws GridException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public long environmentPointer() throws GridException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public String gridName() {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public void close(boolean cancel) {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridInteropTarget projection() throws GridException {
        throw new UnsupportedOperationException(ERR_MSG);
    }

    /** {@inheritDoc} */
    @Override public GridInteropTarget cache(@Nullable String name) throws GridException {
        throw new UnsupportedOperationException(ERR_MSG);
    }
}
