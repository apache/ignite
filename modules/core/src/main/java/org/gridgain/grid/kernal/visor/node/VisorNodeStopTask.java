/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Stops nodes.
 */
@GridInternal
public class VisorNodeStopTask extends VisorMultiNodeTask<Void, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorNodesStopJob job(Void arg) {
        return new VisorNodesStopJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
        return null;
    }

    /**
     * Job that stop node.
     */
    private static class VisorNodesStopJob extends VisorJob<Void, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         */
        private VisorNodesStopJob(Void arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Void arg) throws GridException {
            new Thread(new Runnable() {
                @Override public void run() {
                    Ignition.kill(true);
                }
            }, "grid-stopper").start();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodesStopJob.class, this);
        }
    }
}
