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

package org.apache.ignite.internal.visor.ggfs;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.lang.*;

import java.io.*;
import java.nio.file.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Remove all GGFS profiler logs.
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
                IgniteFs ggfs = ignite.fileSystem(arg);

                Path logsDir = resolveIgfsProfilerLogsDir(ggfs);

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
                throw new IgniteException("Failed to clear profiler logs for GGFS: " + arg, e);
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
