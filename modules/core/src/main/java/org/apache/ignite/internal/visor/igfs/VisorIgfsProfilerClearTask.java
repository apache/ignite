/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.resolveIgfsProfilerLogsDir;

/**
 * Remove all IGFS profiler logs.
 */
@GridInternal
public class VisorIgfsProfilerClearTask extends VisorOneNodeTask<String, IgniteBiTuple<Integer, Integer>> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Job to clear profiler logs.
     */
    private static class VisorIgfsProfilerClearJob extends VisorJob<String, IgniteBiTuple<Integer, Integer>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * Create job with given argument.
         *
         * @param arg Job argument.
         * @param debug Debug flag.
         */
        private VisorIgfsProfilerClearJob(String arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Override protected IgniteBiTuple<Integer, Integer> run(String arg) {
            int deleted = 0;
            int notDeleted = 0;

            try {
                IgniteFileSystem igfs = ignite.fileSystem(arg);

                Path logsDir = resolveIgfsProfilerLogsDir(igfs);

                if (logsDir != null) {
                    PathMatcher matcher = FileSystems.getDefault().getPathMatcher(
                        "glob:igfs-log-" + arg + "-*.csv");

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
                throw new IgniteException("Failed to clear profiler logs for IGFS: " + arg, e);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }

            return new IgniteBiTuple<>(deleted, notDeleted);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorIgfsProfilerClearJob.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override protected VisorIgfsProfilerClearJob job(String arg) {
        return new VisorIgfsProfilerClearJob(arg, debug);
    }
}