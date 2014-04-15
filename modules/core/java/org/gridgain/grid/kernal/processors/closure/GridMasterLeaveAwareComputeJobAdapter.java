/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.closure;

import org.gridgain.grid.compute.*;

/**
 * Job adapter implementing {@link GridComputeJobMasterLeaveAware}.
 */
public abstract class GridMasterLeaveAwareComputeJobAdapter extends GridComputeJobAdapter
    implements GridComputeJobMasterLeaveAware {
    private static final long serialVersionUID = -2338284701653702117L;

    /**
     * No-arg constructor.
     */
    protected GridMasterLeaveAwareComputeJobAdapter() {
        // No-op.
    }
}
