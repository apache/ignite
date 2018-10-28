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

package org.apache.ignite.internal.processors.cache.persistence;

import java.nio.ByteBuffer;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.PageCompression;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.GridCacheManagerAdapter;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.compress.CompressionProcessor.checkCompressionLevelBounds;
import static org.apache.ignite.internal.processors.compress.CompressionProcessor.getDefaultCompressionLevel;

/**
 * Cache compression manager.
 */
public class CacheCompressionManager extends GridCacheManagerAdapter {
    /** */
    private PageCompression pageCompression;

    /** */
    private int pageCompressLevel;

    /** */
    private CompressionProcessor compressProc;

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        DataRegionConfiguration cfg = cctx.dataRegion().config();

        pageCompression = cfg.getPageCompression();

        if (pageCompression != null) {
            if (!cfg.isPersistenceEnabled())
                throw new IgniteCheckedException("Page compression makes sense only with enabled persistence.");

            Integer lvl = cfg.getPageCompressionLevel();
            pageCompressLevel = lvl != null ?
                checkCompressionLevelBounds(lvl, pageCompression) :
                getDefaultCompressionLevel(pageCompression);

            compressProc = cctx.kernalContext().compress();
            compressProc.checkPageCompressionSupported();
        }
    }

    /**
     * @param page Page buffer.
     * @param store Page store.
     * @return Compressed or the same buffer.
     * @throws IgniteCheckedException If failed.
     */
    public ByteBuffer compressPage(ByteBuffer page, PageStore store) throws IgniteCheckedException {
        if (pageCompression == null)
            return page;

        int blockSize = store.getBlockSize();

        if (blockSize <= 0)
            throw new IgniteCheckedException("Failed to detect storage block size on " + U.osString());

        return compressProc.compressPage(page, store.getPageSize(), blockSize, pageCompression, pageCompressLevel);
    }
}
