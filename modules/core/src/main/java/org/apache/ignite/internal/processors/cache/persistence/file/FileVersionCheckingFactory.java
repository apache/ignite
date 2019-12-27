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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.lang.IgniteOutClosure;

/**
 * Checks version in files if it's present on the disk, creates store with latest version otherwise.
 */
public class FileVersionCheckingFactory implements FilePageStoreFactory {
    /** Property to override latest version. Should be used only in tests. */
    public static final String LATEST_VERSION_OVERRIDE_PROPERTY = "file.page.store.latest.version.override";

    /** Latest page store version. */
    public static final int LATEST_VERSION = 2;

    /** Factory to provide I/O interfaces for read/write operations with files. */
    private final FileIOFactory fileIOFactory;

    /**
     * Factory to provide I/O interfaces for read/write operations with files.
     * This is backup factory for V1 page store.
     */
    private FileIOFactory fileIOFactoryStoreV1;

    /** Memory configuration. */
    private final DataStorageConfiguration memCfg;

    /**
     * @param fileIOFactory File IO factory.
     * @param fileIOFactoryStoreV1 File IO factory for V1 page store and for version checking.
     * @param memCfg Memory configuration.
     */
    public FileVersionCheckingFactory(
        FileIOFactory fileIOFactory,
        FileIOFactory fileIOFactoryStoreV1,
        DataStorageConfiguration memCfg
    ) {
        this.fileIOFactory = fileIOFactory;
        this.fileIOFactoryStoreV1 = fileIOFactoryStoreV1;
        this.memCfg = memCfg;
    }

    /** {@inheritDoc} */
    @Override public PageStore createPageStore(
        byte type,
        IgniteOutClosure<Path> pathProvider,
        LongAdderMetric allocatedTracker) throws IgniteCheckedException {
        Path filePath = pathProvider.apply();

        if (!Files.exists(filePath))
            return createPageStore(type, pathProvider, latestVersion(), allocatedTracker);

        try (FileIO fileIO = fileIOFactoryStoreV1.create(filePath.toFile())) {
            int minHdr = FilePageStore.HEADER_SIZE;

            if (fileIO.size() < minHdr)
                return createPageStore(type, pathProvider, latestVersion(), allocatedTracker);

            ByteBuffer hdr = ByteBuffer.allocate(minHdr).order(ByteOrder.nativeOrder());

            fileIO.readFully(hdr);

            hdr.rewind();

            hdr.getLong(); // Read signature

            int ver = hdr.getInt();

            return createPageStore(type, pathProvider, ver, allocatedTracker);
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Error while creating file page store [file=" + filePath.toAbsolutePath() + "]:", e);
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
     * @param ver Version.
     * @param allocatedTracker Metrics updater
     */
    private FilePageStore createPageStore(
        byte type,
        IgniteOutClosure<Path> pathProvider,
        int ver,
        LongAdderMetric allocatedTracker) {

        switch (ver) {
            case FilePageStore.VERSION:
                return new FilePageStore(type, pathProvider, fileIOFactoryStoreV1, memCfg, allocatedTracker);

            case FilePageStoreV2.VERSION:
                return new FilePageStoreV2(type, pathProvider, fileIOFactory, memCfg, allocatedTracker);

            default:
                throw new IllegalArgumentException("Unknown version of file page store: " + ver + " for file [" + pathProvider.apply().toAbsolutePath() + "]");
        }
    }

    /**
     * @param ver Version.
     * @return Header size.
     */
    public int headerSize(int ver) {
        switch (ver) {
            case FilePageStore.VERSION:
                return FilePageStore.HEADER_SIZE;

            case FilePageStoreV2.VERSION:
                return memCfg.getPageSize();

            default:
                throw new IllegalArgumentException("Unknown version of file page store.");
        }
    }
}
