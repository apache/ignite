/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.compute;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

/**
 * Reset compute grid metrics task.
 */
@GridInternal
public class VisorComputeResetMetricsTask extends VisorOneNodeTask<Void, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorComputeResetMetricsJob job(Void arg) {
        return new VisorComputeResetMetricsJob(arg);
    }

    /**
     * Reset compute grid metrics job.
     */
    private static class VisorComputeResetMetricsJob extends VisorJob<Void, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Formal job argument.
         */
        private VisorComputeResetMetricsJob(Void arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Void arg) throws GridException {
            g.resetMetrics();

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorComputeResetMetricsJob.class, this);
        }
    }
}
