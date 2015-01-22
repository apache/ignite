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

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.apache.ignite.internal.visor.*;
import org.apache.ignite.internal.visor.log.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

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
        @Nullable @Override protected Collection<VisorLogFile> run(final IgniteBiTuple<String, String> arg) throws IgniteCheckedException {
            String path = arg.get1();
            String regexp = arg.get2();

            assert path != null;
            assert regexp != null;

            URL url = U.resolveGridGainUrl(path);

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
