// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.compute;

import org.jetbrains.annotations.*;

/**
 * Job adapter implementing {@link GridComputeJobMasterLeaveAware}.
 *
 * @author @java.author
 * @version @java.version
 */
public abstract class GridMasterLeaveAwareComputeJobAdapter extends GridComputeJobAdapter
    implements GridComputeJobMasterLeaveAware {
    /**
     * No-arg constructor.
     */
    protected GridMasterLeaveAwareComputeJobAdapter() {
        // No-op.
    }

    /**
     * @param arg Job argument.
     */
    protected GridMasterLeaveAwareComputeJobAdapter(@Nullable Object arg) {
        super(arg);
    }

    /**
     * @param args Job arguments.
     */
    protected GridMasterLeaveAwareComputeJobAdapter(@Nullable Object... args) {
        super(args);
    }
}
