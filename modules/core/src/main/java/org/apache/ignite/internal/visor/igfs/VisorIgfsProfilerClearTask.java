/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.igfs;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.resolveIgfsProfilerLogsDir;

/**
 * Remove all IGFS profiler logs.
 */
@GridInternal
@GridVisorManagementTask
public class VisorIgfsProfilerClearTask extends VisorOneNodeTask<VisorIgfsProfilerClearTaskArg, VisorIgfsProfilerClearTaskResult> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorIgfsProfilerClearJob job(VisorIgfsProfilerClearTaskArg arg) {
        return new VisorIgfsProfilerClearJob(arg, debug);
    }

    /**
     * Job to clear profiler logs.
     */
    private static class VisorIgfsProfilerClearJob extends VisorJob<VisorIgfsProfilerClearTaskArg, VisorIgfsProfilerClearTaskResult> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorIgfsProfilerClearJob(VisorIgfsProfilerClearTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected VisorIgfsProfilerClearTaskResult run(VisorIgfsProfilerClearTaskArg arg) {
            int deleted = 0;
            int notDeleted = 0;

            try {
                IgniteFileSystem igfs = ignite.fileSystem(arg.getIgfsName());

                Path logsDir = resolveIgfsProfilerLogsDir(igfs);

                if (logsDir != null) {
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(
                        "glob:igfs-log-" + arg.getIgfsName() + "-*.csv");

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

                                    ignite.log().warning("Profiler log file was not deleted: " + p, io);
                                }
                            }
                        }
                    }
                }
            }
            catch (IOException | IllegalArgumentException e) {
                throw new IgniteException("Failed to clear profiler logs for IGFS: " + arg.getIgfsName(), e);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            return new VisorIgfsProfilerClearTaskResult(deleted, notDeleted);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsProfilerClearJob.class, this);
        }
    }
}
