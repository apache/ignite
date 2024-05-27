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

package org.apache.ignite.internal.processors.compress;

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;

/** */
public class CompressionHandler {
    /** */
    private final CompressionProcessor compressProc;

    /** */
    private final int diskPageCompressLevel;

    /** */
    private final DiskPageCompression diskPageCompression;

    /** */
    private CompressionHandler(
        CompressionProcessor compressProc,
        DiskPageCompression diskPageCompression,
        int diskPageCompressLevel
    ) {
        this.diskPageCompression = diskPageCompression;
        this.diskPageCompressLevel = diskPageCompressLevel;
        this.compressProc = compressProc;
    }

    /** */
    private CompressionHandler() {
        diskPageCompression = DiskPageCompression.DISABLED;
        diskPageCompressLevel = 0;
        compressProc = null;
    }

    /**
     * @return Disk page compression algorithm..
     */
    public DiskPageCompression diskPageCompression() {
        return diskPageCompression;
    }

    /**
     * @return Disk page compression level.
     */
    public int diskPageCompressionLevel() {
        return diskPageCompressLevel;
    }

    /**
     * @return {@code true} if disk page compression is enabled.
     */
    public boolean compressionEnabled() {
        return diskPageCompression != DiskPageCompression.DISABLED;
    }

    /**
     * @param page Page buffer.
     * @param store Page store.
     * @return Compressed or the same buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(ByteBuffer page, PageStore store) throws IgniteCheckedException {
        if (diskPageCompression == DiskPageCompression.DISABLED)
            return page;

        int blockSize = store.getBlockSize();

        if (blockSize <= 0)
            throw new IgniteCheckedException("Failed to detect storage block size on " + U.osString());

        return compressProc.compressPage(page, store.getPageSize(), blockSize, diskPageCompression, diskPageCompressLevel);
    }

    /**
     * Creates compression handler.
     *
     * @param ctx Grid kernal context.
     * @param cfg Cache or cache group configuration.
     * @return Compression handler.
     */
    public static CompressionHandler create(
        GridKernalContext ctx,
        CacheConfiguration cfg
    ) throws IgniteCheckedException {
        DiskPageCompression diskPageCompr = cfg.getDiskPageCompression();
        DataStorageConfiguration dsCfg = ctx.config().getDataStorageConfiguration();

        if (ctx.clientNode() || diskPageCompr == DiskPageCompression.DISABLED || !CU.isPersistentCache(cfg, dsCfg))
            return new CompressionHandler();

        CompressionProcessor comprProc = ctx.compress();

        int lvl = CompressionProcessor.getCompressionLevel(cfg.getDiskPageCompressionLevel(), diskPageCompr);

        File dbPath = ctx.pdsFolderResolver().resolveFolders().persistentStoreRootPath();

        assert dbPath != null;

        comprProc.checkPageCompressionSupported(dbPath.toPath(), dsCfg.getPageSize());

        return new CompressionHandler(comprProc, diskPageCompr, lvl);
    }
}
