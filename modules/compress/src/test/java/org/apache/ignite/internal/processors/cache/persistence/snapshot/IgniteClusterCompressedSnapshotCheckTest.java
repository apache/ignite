/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DiskPageCompression;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.junit.runners.Parameterized;

import static org.apache.ignite.configuration.DataStorageConfiguration.DFLT_PAGE_SIZE;

/**
 * Cluster-wide snapshot check procedure tests.
 */
public class IgniteClusterCompressedSnapshotCheckTest extends IgniteClusterSnapshotCheckTest {
    /** Encryption is not supported with page compression. */
    @Parameterized.Parameters(name = "Encryption={0}")
    public static Collection<Boolean> parameters() {
        return Collections.singletonList(false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.getDataStorageConfiguration().setPageSize(4 * DFLT_PAGE_SIZE);
        cfg.getDataStorageConfiguration().getDefaultDataRegionConfiguration()
            .setMaxSize(1024L * 1024 * 1024);

        return cfg;
    }

    /** */
    @Override protected <K, V> CacheConfiguration<K, V> txCacheConfig(CacheConfiguration<K, V> ccfg) {
        CacheConfiguration<K, V> ccfg0 = super.txCacheConfig(ccfg);

        ccfg0.setDiskPageCompression(DiskPageCompression.SNAPPY);

        return ccfg0;
    }

    /** */
    @Override protected void decompressPage(Ignite grid, ByteBuffer pageBuf, PageStore pageStore) throws IgniteCheckedException {
        long pageAddr = GridUnsafe.bufferAddress(pageBuf);

        assertTrue(PageIO.getCompressionType(pageAddr) != CompressionProcessor.UNCOMPRESSED_PAGE);

        ((IgniteEx)grid).context().compress().decompressPage(pageBuf, pageStore.getPageSize());
    }

    /** */
    @Override protected void compressPage(Ignite grid, ByteBuffer pageBuf, PageStore pageStore) throws IgniteCheckedException {
        ByteBuffer compressedPageBuf = ((IgniteEx)grid).context().compress().compressPage(pageBuf, pageStore.getPageSize(),
            pageStore.getBlockSize(), DiskPageCompression.SNAPPY, 0);

        if (compressedPageBuf != pageBuf) {
            pageBuf = compressedPageBuf;

            PageIO.setCrc(pageBuf, 0);
        }
    }
}
