/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.nio.file.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Remove all GGFS profiler logs.
 */
@GridInternal
public class VisorGgfsProfilerClearTask extends VisorOneNodeTask<String, IgniteBiTuple<Integer, Integer>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job to clear profiler logs.
     */
    private static class VisorGgfsProfilerClearJob extends VisorJob<String, IgniteBiTuple<Integer, Integer>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         */
        private VisorGgfsProfilerClearJob(String arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Integer, Integer> run(String arg) throws GridException {
            int deleted = 0;
            int notDeleted = 0;

            try {
                IgniteFs ggfs = g.fileSystem(arg);

                Path logsDir = resolveGgfsProfilerLogsDir(ggfs);

                if (logsDir != null) {
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(
                        "glob:ggfs-log-" + arg + "-*.csv");

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
                throw new GridException("Failed to clear profiler logs for GGFS: " + arg, ioe);
            }

            return new IgniteBiTuple<>(deleted, notDeleted);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorGgfsProfilerClearJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorGgfsProfilerClearJob job(String arg) {
        return new VisorGgfsProfilerClearJob(arg);
    }
}
