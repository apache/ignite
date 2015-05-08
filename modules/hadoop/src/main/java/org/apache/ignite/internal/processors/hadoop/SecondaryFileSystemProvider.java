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

package org.apache.ignite.internal.processors.hadoop;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;

/**
 * Encapsulates logic of secondary filesystem creation.
 */
public class SecondaryFileSystemProvider {
    /** Configuration of the secondary filesystem, never null. */
    private final Configuration cfg = new Configuration();

    /** The secondary filesystem URI, never null. */
    private final URI uri;

    /** Optional user name to log into secondary filesystem with. */
    private @Nullable final String userName;

    /**
     * Creates new provider with given config parameters. The configuration URL is optional. The filesystem URI must be
     * specified either explicitly or in the configuration provided.
     *
     * @param secUri the secondary Fs URI (optional). If not given explicitly, it must be specified as "fs.defaultFS"
     * property in the provided configuration.
     * @param secConfPath the secondary Fs path (file path on the local file system, optional).
     * See {@link IgniteUtils#resolveIgniteUrl(String)} on how the path resolved.
     * @param userName User name.
     * @throws IOException
     */
    public SecondaryFileSystemProvider(final @Nullable String secUri,
        final @Nullable String secConfPath, @Nullable String userName) throws IOException {
        this.userName = userName;

        if (secConfPath != null) {
            URL url = U.resolveIgniteUrl(secConfPath);

            if (url == null) {
                // If secConfPath is given, it should be resolvable:
                throw new IllegalArgumentException("Failed to resolve secondary file system " +
                    "configuration path: " + secConfPath);
            }

            cfg.addResource(url);
        }

        // if secondary fs URI is not given explicitly, try to get it from the configuration:
        if (secUri == null)
            uri = FileSystem.getDefaultUri(cfg);
        else {
            try {
                uri = new URI(secUri);
            }
            catch (URISyntaxException use) {
                throw new IOException("Failed to resolve secondary file system URI: " + secUri);
            }
        }

        // Disable caching:
        String prop = String.format("fs.%s.impl.disable.cache", uri.getScheme());

        cfg.setBoolean(prop, true);
    }

    /**
     * @return {@link org.apache.hadoop.fs.FileSystem}  instance for this secondary Fs.
     * @throws IOException
     */
    public FileSystem createFileSystem() throws IOException {
        final FileSystem fileSys;

        if (userName == null)
            fileSys = FileSystem.get(uri, cfg);
        else {
            try {
                fileSys = FileSystem.get(uri, cfg, userName);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();

                throw new IOException("Failed to create file system due to interrupt.", e);
            }
        }

        return fileSys;
    }

    /**
     * @return {@link org.apache.hadoop.fs.AbstractFileSystem} instance for this secondary Fs.
     * @throws IOException
     */
    public AbstractFileSystem createAbstractFileSystem() throws IOException {
        return AbstractFileSystem.get(uri, cfg);
    }

    /**
     * @return the secondary fs URI, never null.
     */
    public URI uri() {
        return uri;
    }
}
