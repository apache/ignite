/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;

import java.util.*;

/**
 * Hadoop accelerator context.
 */
public class GridHadoopContext {
    /** Kernal context. */
    private GridKernalContext ctx;

    /** Job tracker manager. */
    private GridHadoopJobTrackerManager jobTracker;

    /** Managers list. */
    private List<GridHadoopManager> mgrs = new ArrayList<>();

    /**
     * @param ctx Kernal context.
     */
    public GridHadoopContext(
        GridKernalContext ctx,
        GridHadoopJobTrackerManager jobTracker
    ) {
        this.ctx = ctx;

        this.jobTracker = add(jobTracker);
    }

    /**
     * Gets list of managers.
     *
     * @return List of managers.
     */
    public List<GridHadoopManager> managers() {
        return mgrs;
    }

    /**
     * Gets kernal context.
     *
     * @return Grid kernal context instance.
     */
    public GridKernalContext kernalContext() {
        return ctx;
    }

    /**
     * @return Jon tracker manager instance.
     */
    public GridHadoopJobTrackerManager jobTracker() {
        return jobTracker;
    }

    /**
     * Gets local node ID. Shortcut for {@code kernalContext().localNodeId()}.
     *
     * @return Local node ID.
     */
    public UUID localNodeId() {
        return ctx.localNodeId();
    }

    /**
     * Adds manager to managers list.
     *
     * @param mgr Manager to add.
     * @return Added manager.
     */
    private <T extends GridHadoopManager> T add(T mgr) {
        mgrs.add(mgr);

        return mgr;
    }
}
