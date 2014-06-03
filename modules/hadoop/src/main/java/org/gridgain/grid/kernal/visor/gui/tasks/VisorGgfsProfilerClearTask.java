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
import org.gridgain.grid.kernal.visor.cmd.*;

import static org.gridgain.grid.kernal.visor.gui.tasks.VisorHadoopTaskUtilsEnt.resolveGgfsProfilerLogsDir;

import java.io.*;
import java.nio.file.*;

/**
 * Remove all GGFS profiler logs.
 */
@GridInternal
public class VisorGgfsProfilerClearTask extends VisorOneNodeTask<VisorOneNodeNameArg,
    VisorGgfsProfilerClearTask.VisorGgfsProfilerClearTaskResult> {
    /** GGFS profiler task result. */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorGgfsProfilerClearTaskResult implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final int deleted;

        /** */
        private final int notDeleted;

        public VisorGgfsProfilerClearTaskResult(int deleted, int failed) {
            this.deleted = deleted;
            this.notDeleted = failed;
        }

        /**
         * @return Number of deleted log files.
         */
        public int deleted() {
            return deleted;
        }

        /**
         * @return Number of log files that were not deleted.
         */
        public int notDeleted() {
            return notDeleted;
        }
    }

    @SuppressWarnings("PublicInnerClass")
    public static class VisorGgfsProfilerClearJob
        extends VisorOneNodeJob<VisorOneNodeNameArg, VisorGgfsProfilerClearTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Create job with given argument. */
        public VisorGgfsProfilerClearJob(VisorOneNodeNameArg arg) {
            super(arg);
        }

        @Override protected VisorGgfsProfilerClearTaskResult run(VisorOneNodeNameArg arg) throws GridException {
            int deleted = 0;
            int notDeleted = 0;

            try {
                GridGgfs ggfs = g.ggfs(arg.name());

                Path logsDir = resolveGgfsProfilerLogsDir(ggfs);

                if (logsDir != null) {
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(
                        "glob:ggfs-log-" + arg.name() + "-*.csv");

                    try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(logsDir)) {
                        for (Path p : dirStream) {
                            if (matcher.matches(p.getFileName())) {
                                try {
                                    Files.delete(p); // Try to delete file.

                                    if (Files.exists(p)) // Checks if it still exists.
                                        notDeleted++;
                                    else
                                        deleted++;
                                }
                                catch (NoSuchFileException ignored) {
                                    // Files was already deleted, skip.
                                }
                                catch (IOException io) {
                                    notDeleted++;

                                    g.log().warning("Profiler log file was not deleted: " + p, io);
                                }
                            }
                        }
                    }
                }
            }
            catch (IOException | IllegalArgumentException ioe) {
                throw new GridException("Failed to clear profiler logs for GGFS: " + arg.name(), ioe);
            }

            return new VisorGgfsProfilerClearTaskResult(deleted, notDeleted);
        }
    }

    @Override protected VisorGgfsProfilerClearJob job(VisorOneNodeNameArg arg) {
        return new VisorGgfsProfilerClearJob(arg);
    }
}
