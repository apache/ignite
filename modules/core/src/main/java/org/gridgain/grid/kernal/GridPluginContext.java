/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.design.plugin.*;

/**
 *
 */
public class GridPluginContext implements PluginContext {
    /** */
    private final GridKernalContext ctx;

    /**
     * @param ctx Kernal context.
     */
    public GridPluginContext(GridKernalContext ctx) {
        this.ctx = ctx;
    }

    /**
     * @return Kernal context.
     */
    public GridKernalContext context() {
        return ctx;
    }
}
