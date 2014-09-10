/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.interop;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

/**
 * Interop processor adapter.
 */
public abstract class GridInteropProcessorAdapter extends GridProcessorAdapter implements GridInteropProcessor {
    /**
     * Constructor.
     *
     * @param ctx Context.
     */
    protected GridInteropProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public String gridName() {
        return ctx.gridName();
    }

    /** {@inheritDoc} */
    @Override public void close(boolean cancel) {
        GridGain.stop(ctx.gridName(), cancel);
    }
}
