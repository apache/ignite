/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.typedef.internal.*;

import static org.gridgain.grid.kernal.visor.cmd.VisorTaskUtils.*;

/**
 * Task for reset metrics for specified streamer.
 */
@GridInternal
public class VisorStreamerMetricsResetTask extends VisorOneNodeTask<String, Void> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorStreamerMetricsResetJob job(String arg) {
        return new VisorStreamerMetricsResetJob(arg);
    }

    /**
     * Job that reset streamer metrics.
     */
    private static class VisorStreamerMetricsResetJob extends VisorJob<String, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Streamer name.
         */
        private VisorStreamerMetricsResetJob(String arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(String streamerName) throws GridException {
            try {
                GridStreamer streamer = g.streamer(streamerName);

                streamer.resetMetrics();

                return null;
            }
            catch (IllegalArgumentException iae) {
                throw new GridException("Failed to reset metrics for streamer: " + escapeName(streamerName) +
                    " on node: " + g.localNode().id(), iae);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorStreamerMetricsResetJob.class, this);
        }
    }
}
