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

package org.apache.ignite.internal.processors.cache;

import java.io.File;
import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.compress.CompressionProcessor.checkCompressionLevelBounds;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.getDefaultCompressionLevel;

/**
 * Cache compression manager.
 */
public class CacheCompressionManager extends GridCacheManagerAdapter {
    /** */
    private DiskPageCompression diskPageCompression;

    /** */
    private int diskPageCompressLevel;

    /** */
    private CompressionProcessor compressProc;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        if (cctx.kernalContext().clientNode()) {
            diskPageCompression = DiskPageCompression.DISABLED;

            return;
        }

        compressProc = cctx.kernalContext().compress();

        CacheConfiguration cfg = cctx.config();

        diskPageCompression = cctx.kernalContext().config().isClientMode() ? null : cfg.getDiskPageCompression();

        if (diskPageCompression != DiskPageCompression.DISABLED) {
            if (!cctx.dataRegion().config().isPersistenceEnabled())
                throw new IgniteCheckedException("Disk page compression makes sense only with enabled persistence.");

            Integer lvl = cfg.getDiskPageCompressionLevel();
            diskPageCompressLevel = lvl != null ?
                checkCompressionLevelBounds(lvl, diskPageCompression) :
                getDefaultCompressionLevel(diskPageCompression);

            DataStorageConfiguration dsCfg = cctx.kernalContext().config().getDataStorageConfiguration();

            File dbPath = cctx.kernalContext().pdsFolderResolver().resolveFolders().persistentStoreRootPath();

            assert dbPath != null;

            compressProc.checkPageCompressionSupported(dbPath.toPath(), dsCfg.getPageSize());

            if (log.isInfoEnabled()) {
                log.info("Disk page compression is enabled [cache=" + cctx.name() +
                    ", compression=" + diskPageCompression + ", level=" + diskPageCompressLevel + "]");
            }
        }
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
}
