// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.examples.hpc.masterfailure;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.compute.GridComputeTaskSessionScope.*;

/**
 * A job which is executed on worked node. It simply increments internal counter until cancelled. Note that
 * this job implements {@link org.gridgain.grid.compute.GridComputeJobMasterLeaveAware} interface and in case master node leaves the gird,
 * intermediate job state is saved as a checkpoint.
 *
 * @author @java.author
 * @version @java.version
 */
@GridComputeTaskSessionFullSupport
public class GridMasterLeaveJob extends GridComputeJobAdapter implements GridComputeJobMasterLeaveAware {
    /** Grid where the job is being executed. */
    @GridInstanceResource
    private Grid grid;

    /** Task session. */
    @GridTaskSessionResource
    private GridComputeTaskSession ses;

    /** Checkpoint key. */
    private String key;

    /** Counter. */
    private final AtomicInteger state = new AtomicInteger();

    /**
     * Constructor.
     *
     * @param key Job key.
     */
    public GridMasterLeaveJob(String key) {
        this.key = key;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public Object execute() throws GridException {
        // Try loading previously saved state.
        Integer savedState = load(key);

        if (savedState != null) {
            state.set(savedState);

            System.out.println(">>> Job execution started from the previously saved state: " + state);
        }
        else
            System.out.println(">>> Job execution started for the first time with state: " + state);

        try {
            while (!isCancelled()) {
                System.out.println("Incremented job state: " + state.incrementAndGet());

                Thread.sleep(500);
            }
        }
        catch (InterruptedException ignore) {
            // No-op.
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onMasterNodeLeft(GridComputeTaskSession ses)
        throws GridException {
        // Cancel job execution and save state.
        cancel();

        System.out.println(">>> Saving intermediate job state due to master leave as a checkpoint for 5 minutes: " + state);

        save(key, state.get());
    }

    /**
     * Save job state.
     *
     * @param key Job key which allows.
     * @param state State.
     * @throws GridException If result save failed.
     */
    private void save(String key, Integer state) throws GridException {
        // Save checkpoint and keep it for 5 minutes.
        ses.saveCheckpoint(key, state, GLOBAL_SCOPE, 5 * 60 * 1000);
    }

    /**
     * Load job state.
     *
     * @param key Key.
     * @return Result.
     * @throws GridException If result loading failed.
     */
    private Integer load(String key) throws GridException {
        return (Integer)ses.loadCheckpoint(key);
    }
}
