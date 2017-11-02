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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.MemoryConfiguration;

/**
 * Checks version in files if it's present on the disk, creates store with latest version otherwise.
 */
public class FileVersionCheckingFactory implements FilePageStoreFactory {
    /** Property to override latest version. Should be used only in tests. */
    public static final String LATEST_VERSION_OVERRIDE_PROPERTY = "file.page.store.latest.version.override";

    /** Latest page store version. */
    public final static int LATEST_VERSION = 2;

    /** Factory to provide I/O interfaces for read/write operations with files. */
    private final FileIOFactory fileIOFactory;

    /** Memory configuration. */
    private final MemoryConfiguration memCfg;

    /**
     * @param fileIOFactory File io factory.
     * @param memCfg Memory configuration.
     */
    public FileVersionCheckingFactory(
        FileIOFactory fileIOFactory, MemoryConfiguration memCfg) {
        this.fileIOFactory = fileIOFactory;
        this.memCfg = memCfg;
    }

    /** {@inheritDoc} */
    @Override public FilePageStore createPageStore(byte type, File file) throws IgniteCheckedException {
        if (!file.exists())
            return createPageStore(type, file, latestVersion());

        try (FileIO fileIO = fileIOFactory.create(file)) {
            int minHdr = FilePageStore.HEADER_SIZE;

            if (fileIO.size() < minHdr)
                return createPageStore(type, file, latestVersion());

            ByteBuffer hdr = ByteBuffer.allocate(minHdr).order(ByteOrder.LITTLE_ENDIAN);

            while (hdr.remaining() > 0)
                fileIO.read(hdr);

            hdr.rewind();

            hdr.getLong(); // Read signature

            int ver = hdr.getInt();

            return createPageStore(type, file, ver);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Error while creating file page store [file=" + file + "]:", e);
        }
    }

    /**
     * Resolves latest page store version.
     */
    public int latestVersion() {
        int latestVer = LATEST_VERSION;

        try {
            latestVer = Integer.parseInt(System.getProperty(LATEST_VERSION_OVERRIDE_PROPERTY));
        } catch (NumberFormatException e) {
            // No override.
        }

        return latestVer;
    }

    /**
     * Instantiates specific version of FilePageStore.
     *
     * @param type Type.
     * @param file File.
     * @param ver Version.
     */
    public FilePageStore createPageStore(byte type, File file, int ver) throws IgniteCheckedException {
        switch (ver) {
            case FilePageStore.VERSION:
                return new FilePageStore(type, file, fileIOFactory, memCfg);

            case FilePageStoreV2.VERSION:
                return new FilePageStoreV2(type, file, fileIOFactory, memCfg);

            default:
                throw new IllegalArgumentException("Unknown version of file page store: " + ver);
        }
    }
}
