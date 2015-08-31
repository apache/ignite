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

package org.apache.ignite.internal.visor.file;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorOneNodeTask;
import org.apache.ignite.internal.visor.log.VisorLogFile;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.LOG_FILES_COUNT_LIMIT;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.matchedFiles;

/**
 * Get list files matching filter.
 */
@GridInternal
public class VisorLatestTextFilesTask extends VisorOneNodeTask<IgniteBiTuple<String, String>, Collection<VisorLogFile>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** {@inheritDoc} */
    @Override protected VisorLatestTextFilesJob job(IgniteBiTuple<String, String> arg) {
        return new VisorLatestTextFilesJob(arg, debug);
    }

    /**
     * Job that gets list of files.
     */
    private static class VisorLatestTextFilesJob extends VisorJob<IgniteBiTuple<String, String>, Collection<VisorLogFile>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Folder and regexp.
         * @param debug Debug flag.
         */
        private VisorLatestTextFilesJob(IgniteBiTuple<String, String> arg, boolean debug) {
            super(arg, debug);
        }

        /** {@inheritDoc} */
        @Nullable @Override protected Collection<VisorLogFile> run(final IgniteBiTuple<String, String> arg) {
            String path = arg.get1();
            String regexp = arg.get2();

            assert path != null;
            assert regexp != null;

            URL url = U.resolveIgniteUrl(path);

            if (url == null)
                return null;

            try {
                File folder = new File(url.toURI());

                List<VisorLogFile> files = matchedFiles(folder, regexp);

                if (files.isEmpty())
                    return null;

                if (files.size() > LOG_FILES_COUNT_LIMIT)
                    files = new ArrayList<>(files.subList(0, LOG_FILES_COUNT_LIMIT));

                return files;
            }
            catch (Exception ignored) {
                return null;
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLatestTextFilesJob.class, this);
        }
    }
}