/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.*;

/**
 * Rest processor.
 */
public abstract class GridRestProcessorAdapter extends GridProcessorAdapter {
    /** */
    private boolean tcp;

    /**
     * @param ctx Kernal context.
     */
    protected GridRestProcessorAdapter(GridKernalContext ctx) {
        super(ctx);
    }

    /**
     * @param tcp Whether this protocol handles TCP protocol (HTTP otherwise).
     */
    public void tcp(boolean tcp) {
        this.tcp = tcp;
    }

    /**
     * @return Whether this protocol handles TCP protocol (HTTP otherwise).
     */
    public boolean tcp() {
        return tcp;
    }
}
