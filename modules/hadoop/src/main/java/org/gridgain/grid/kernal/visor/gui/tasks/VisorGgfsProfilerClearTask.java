/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.tasks;

import org.gridgain.grid.GridException;
import org.gridgain.grid.ggfs.GridGgfs;
import org.gridgain.grid.kernal.processors.task.GridInternal;
import org.gridgain.grid.kernal.visor.cmd.VisorJob;
import org.gridgain.grid.kernal.visor.cmd.VisorOneNodeArg;
import org.gridgain.grid.kernal.visor.cmd.VisorOneNodeJob;
import org.gridgain.grid.kernal.visor.cmd.VisorOneNodeTask;
import org.gridgain.grid.lang.GridBiTuple;
import org.jetbrains.annotations.Nullable;
import static org.gridgain.grid.kernal.visor.gui.tasks.VisorHadoopTaskUtilsEnt.resolveGgfsProfilerLogsDir;

import java.io.IOException;
import java.nio.file.*;
import java.util.Iterator;
import java.util.UUID;

/**
 * Remove all GGFS profiler logs.
 */
@GridInternal
public class VisorGgfsProfilerClearTask extends VisorOneNodeTask<VisorGgfsProfilerClearTask.VisorGgfsProfilerClearArg,
    VisorGgfsProfilerClearTask.VisorGgfsProfilerClearTaskResult> {
    /**
     * Arguments for {@link org.gridgain.grid.kernal.visor.gui.tasks.VisorGgfsProfilerClearTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorGgfsProfilerClearArg extends VisorOneNodeArg {
        /** */
        private static final long serialVersionUID = 0L;

        public final String ggfsName;

        /**
         * @param nodeId Node Id.
         * @param ggfsName
         */
        protected VisorGgfsProfilerClearArg(UUID nodeId, String ggfsName) {
            super(nodeId);

            this.ggfsName = ggfsName;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorGgfsProfilerClearTaskResult extends GridBiTuple<Integer, Integer> {
        /** */
        private static final long serialVersionUID = 0L;

        public VisorGgfsProfilerClearTaskResult(@Nullable Integer deleted, @Nullable Integer failed) {
            super(deleted, failed);
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorGgfsProfilerClearJob
        extends VisorOneNodeJob<VisorGgfsProfilerClearArg, VisorGgfsProfilerClearTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with specified argument.
         *
         * @param arg Job argument.
         */
        protected VisorGgfsProfilerClearJob(VisorGgfsProfilerClearArg arg) {
            super(arg);
        }

        @Override
        protected VisorGgfsProfilerClearTaskResult run(VisorGgfsProfilerClearArg arg) throws GridException, IOException {
            int deleted = 0;
            int failed = 0;

            try {
                GridGgfs ggfs = g.ggfs(arg.ggfsName);

                Path logsDir = resolveGgfsProfilerLogsDir(ggfs);

                if (logsDir != null) {
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(
                        "glob:ggfs-log-" + arg.ggfsName + "-*.csv");

                    DirectoryStream<Path> dirStream = Files.newDirectoryStream(logsDir);

                    try {
                        Iterator<Path> itr = dirStream.iterator();

                        while(itr.hasNext()) {
                            Path p = itr.next();

                            if (matcher.matches(p.getFileName()))
                                try {
                                    Files.delete(p); // Try to delete file.

                                    if (Files.exists(p)) // Checks if it still exists.
                                        failed ++;
                                    else
                                        deleted ++;
                                }
                                catch (NoSuchFileException nsfe) {} // Files was already deleted, skip.
                                catch (IOException io) {
                                    failed ++;

                                    g.log().warning("Profiler log file was not deleted: " + p, io);
                                }
                        }
                    }
                    finally {
                        dirStream.close();
                    }
                }
            }
            catch (IllegalArgumentException iae) {
                throw new GridException("Failed to clear profiler logs for GGFS: " + arg.ggfsName, iae);
            }

            return new VisorGgfsProfilerClearTaskResult(deleted, failed);
        }
    }

    @Override
    protected VisorJob<VisorGgfsProfilerClearArg, VisorGgfsProfilerClearTaskResult> job(VisorGgfsProfilerClearArg arg) {
        return new VisorGgfsProfilerClearJob(arg);
    }
}
