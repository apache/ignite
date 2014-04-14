/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop;

import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.hadoop.jobtracker.*;
import org.gridgain.grid.kernal.processors.hadoop.shuffle.*;
import org.gridgain.grid.kernal.processors.hadoop.taskexecutor.*;

import java.util.*;

/**
 * Hadoop accelerator context.
 */
public class GridHadoopContext {
    /** Kernal context. */
    private GridKernalContext ctx;

    /** Job tracker. */
    private GridHadoopJobTracker jobTracker;

    /** */
    private GridHadoopTaskExecutor taskExecutor;

    /** */
    private GridHadoopShuffle shuffle;

    /** Managers list. */
    private List<GridHadoopComponent> components = new ArrayList<>();

    /**
     * @param ctx Kernal context.
     */
    public GridHadoopContext(
        GridKernalContext ctx,
        GridHadoopJobTracker jobTracker,
        GridHadoopTaskExecutor taskExecutor,
        GridHadoopShuffle shuffle
    ) {
        this.ctx = ctx;

        this.jobTracker = add(jobTracker);
        this.taskExecutor = add(taskExecutor);
        this.shuffle = add(shuffle);

        GridHadoopConfiguration hcfg = ctx.config().getHadoopConfiguration();


    }

    /**
     * Gets list of managers.
     *
     * @return List of managers.
     */
    public List<GridHadoopComponent> components() {
        return components;
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
     * @return Jon tracker instance.
     */
    public GridHadoopJobTracker jobTracker() {
        return jobTracker;
    }

    /**
     * @return Task executor.
     */
    public GridHadoopTaskExecutor taskExecutor() {
        return taskExecutor;
    }

    /**
     * @return Shuffle.
     */
    public GridHadoopShuffle shuffle() {
        return shuffle;
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
     * @return Map-reduce planner.
     */
    public GridHadoopMapReducePlanner planner() {
        return null;
    }

    /**
     * @return Job factory.
     */
    public GridHadoopJobFactory jobFactory() {
        return null;
    }

    /**
     * Adds component.
     *
     * @param c Component to add.
     * @return Added manager.
     */
    private <C extends GridHadoopComponent> C add(C c) {
        components.add(c);

        return c;
    }
}
