/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.node;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Restarts nodes.
 */
@GridInternal
public class VisorNodeRestartTask extends VisorMultiNodeTask<Void, Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Nullable @Override protected Void reduce0(List<ComputeJobResult> results) throws GridException {
        return null;
    }

    /**
     * Job that restart node.
     */
    private static class VisorNodesRestartJob extends VisorJob<Void, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         * @param debug Debug flag.
         */
        private VisorNodesRestartJob(Void arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Void arg) throws GridException {
            new Thread(new Runnable() {
                @Override public void run() {
                    Ignition.restart(true);
                }
            }, "grid-restarter").start();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorNodesRestartJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorNodesRestartJob job(Void arg) {
        return new VisorNodesRestartJob(arg, debug);
    }
}
