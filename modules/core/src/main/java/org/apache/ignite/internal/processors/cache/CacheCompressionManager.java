/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;

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
        diskPageCompression = DiskPageCompression.DISABLED;
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
