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

package org.apache.ignite.internal.processors.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFs;
import org.apache.hadoop.fs.DelegateToFileSystem;
import org.apache.hadoop.fs.FsServerDefaults;
import org.apache.hadoop.fs.local.LocalConfigKeys;

import static org.apache.hadoop.fs.FsConstants.LOCAL_FS_URI;

/**
 * Local file system replacement for Hadoop jobs.
 */
public class HadoopLocalFileSystemV2 extends ChecksumFs {
    /**
     * Creates new local file system.
     *
     * @param cfg Configuration.
     * @throws IOException If failed.
     * @throws URISyntaxException If failed.
     */
    public HadoopLocalFileSystemV2(Configuration cfg) throws IOException, URISyntaxException {
        super(new DelegateFS(cfg));
    }

    /**
     * Creates new local file system.
     *
     * @param uri URI.
     * @param cfg Configuration.
     * @throws IOException If failed.
     * @throws URISyntaxException If failed.
     */
    public HadoopLocalFileSystemV2(URI uri, Configuration cfg) throws IOException, URISyntaxException {
        this(cfg);
    }

    /**
     * Delegate file system.
     */
    private static class DelegateFS extends DelegateToFileSystem {
        /**
         * Creates new local file system.
         *
         * @param cfg Configuration.
         * @throws IOException If failed.
         * @throws URISyntaxException If failed.
         */
        public DelegateFS(Configuration cfg) throws IOException, URISyntaxException {
            super(LOCAL_FS_URI, new HadoopRawLocalFileSystem(), cfg, LOCAL_FS_URI.getScheme(), false);
        }

        /** {@inheritDoc} */
        @Override public int getUriDefaultPort() {
            return -1;
        }

        /** {@inheritDoc} */
        @Override public FsServerDefaults getServerDefaults() throws IOException {
            return LocalConfigKeys.getServerDefaults();
        }

        /** {@inheritDoc} */
        @Override public boolean isValidName(String src) {
            return true;
        }
    }
}