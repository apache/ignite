/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.ggfs.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.cmd.*;

import java.util.*;

/**
 * Task to set GGFS instance sampling state.
 */
@GridInternal
public class VisorSetGgfsSamplingStateTask
    extends VisorOneNodeTask<VisorSetGgfsSamplingStateTask.VisorSetGgfsSamplingStateArg, Void> {
    /**
     * Arguments for {@link VisorSetGgfsSamplingStateTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorSetGgfsSamplingStateArg extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final String ggfsName;

        /** */
        private final Boolean state;

        /**
         * @param nodeId Node Id.
         * @param ggfsName GGFS instance name.
         * @param state If {@code true} then enable GGFS sampling.
         *              If {@code false} then disable GGFS sampling.
         *              If {@code null} then clear sampling flag.
         */
        public VisorSetGgfsSamplingStateArg(UUID nodeId, String ggfsName, Boolean state) {
            super(nodeId);

            this.ggfsName = ggfsName;
            this.state = state;
        }
    }

    /**
     * Job that perform parsing of GGFS profiler logs.
     */
    private static class VisorSetGgfsSamplingStateJob extends VisorOneNodeJob<VisorSetGgfsSamplingStateArg, Void> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         */
        public VisorSetGgfsSamplingStateJob(VisorSetGgfsSamplingStateArg arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected Void run(VisorSetGgfsSamplingStateArg arg) throws GridException {
            try {
                ((GridGgfsEx) g.ggfs(arg.ggfsName)).globalSampling(arg.state);

                return null;
            }
            catch (IllegalArgumentException iae) {
                throw new GridException("Failed to set sampling state for GGFS: " + arg.ggfsName, iae);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorSetGgfsSamplingStateJob job(VisorSetGgfsSamplingStateArg arg) {
        return new VisorSetGgfsSamplingStateJob(arg);
    }
}
