/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.ggfs;

import org.apache.ignite.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
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
        return new VisorGgfsResetMetricsJob(arg, debug);
    }

    /**
     * Job that reset GGFS metrics.
     */
    private static class VisorGgfsResetMetricsJob extends VisorJob<Set<String>, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg GGFS names.
         * @param debug Debug flag.
         */
        private VisorGgfsResetMetricsJob(Set<String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected Void run(Set<String> ggfsNames) throws IgniteCheckedException {
            for (String ggfsName: ggfsNames)
                try {
                    g.fileSystem(ggfsName).resetMetrics();
                }
                catch (IllegalArgumentException iae) {
                    throw new IgniteCheckedException("Failed to reset metrics for GGFS: " + ggfsName, iae);
                }

            return null;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorGgfsResetMetricsJob.class, this);
        }
    }
}
