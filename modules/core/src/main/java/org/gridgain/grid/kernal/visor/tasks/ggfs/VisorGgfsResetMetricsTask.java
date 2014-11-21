/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.tasks.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.tasks.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Resets GGFS metrics.
 */
@GridInternal
public class VisorGgfsResetMetricsTask extends VisorOneNodeTask<Set<String>, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorGgfsResetMetricsJob job(Set<String> arg) {
        return new VisorGgfsResetMetricsJob(arg);
    }

    /**
     * Job that reset GGFS metrics.
     */
    private static class VisorGgfsResetMetricsJob extends VisorJob<Set<String>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg GGFS names.
         */
        private VisorGgfsResetMetricsJob(Set<String> arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Set<String> ggfsNames) throws GridException {
            for (String ggfsName: ggfsNames)
                try {
                    g.ggfs(ggfsName).resetMetrics();
                }
                catch (IllegalArgumentException iae) {
                    throw new GridException("Failed to reset metrics for GGFS: " + ggfsName, iae);
                }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorGgfsResetMetricsJob.class, this);
        }
    }
}
