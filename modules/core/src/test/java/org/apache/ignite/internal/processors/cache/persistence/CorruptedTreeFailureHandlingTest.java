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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.tree.CorruptedTreeException;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.DataPageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.FastCrc;
import org.apache.ignite.internal.processors.cache.tree.AbstractDataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.DataLeafIO;
import org.apache.ignite.internal.processors.cache.tree.mvcc.data.MvccDataLeafIO;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** */
public class CorruptedTreeFailureHandlingTest extends GridCommonAbstractTest implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private static final int CACHE_ENTRIES = 10;

    /** Partition file with corrupted page. */
    private final AtomicReference<File> fileRef = new AtomicReference<>();

    /** Link to corrupted page. */
    private final AtomicLong linkRef = new AtomicLong();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setConsistentId(gridName);

        DataStorageConfiguration dataStorageConfiguration = new DataStorageConfiguration();

        dataStorageConfiguration.getDefaultDataRegionConfiguration().setPersistenceEnabled(true);
        dataStorageConfiguration.setFileIOFactory(new CollectLinkFileIOFactory());

        cfg.setDataStorageConfiguration(dataStorageConfiguration);

        cfg.setCacheConfiguration(new CacheConfiguration<>()
            .setName(DEFAULT_CACHE_NAME)
            .setAffinity(new RendezvousAffinityFunction().setPartitions(1))
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
        );

        return cfg;
    }

    /** */
    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /** */
    @Test
    public void testCorruptedPage() throws Exception {
        IgniteEx srv = startGrid(0);

        File diagnosticDir = new File(srv.context().config().getWorkDirectory(), "diagnostic");

        FileUtils.deleteDirectory(diagnosticDir);

        srv.cluster().active(true);

        IgniteCache<Integer, Integer> cache = srv.getOrCreateCache(DEFAULT_CACHE_NAME);

        for (int i = 0; i < 10; i++)
            cache.put(i, i);

        int pageSize = srv.configuration().getDataStorageConfiguration().getPageSize();

        int grpId = srv.context().cache().cacheGroups().stream().filter(
            context -> context.cacheOrGroupName().equals(DEFAULT_CACHE_NAME)
        ).findAny().orElseThrow(() -> new RuntimeException("Cache group not found")).groupId();

        stopGrid(0, false);

        // Node is stopped, we're ready to corrupt partition data.
        long link = linkRef.get();
        long pageId = PageIdUtils.pageId(link);
        int itemId = PageIdUtils.itemId(link);

        ByteBuffer pageBuf = ByteBuffer.allocateDirect(pageSize);

        OpenOption[] options = {StandardOpenOption.READ, StandardOpenOption.WRITE};
        try (RandomAccessFileIO fileIO = new RandomAccessFileIO(fileRef.get(), options)) {
            DataPageIO dataPageIO = DataPageIO.VERSIONS.latest();

            long pageOff = pageSize + PageIdUtils.pageIndex(pageId) * pageSize;

            // Read index page.
            fileIO.position(pageOff);
            fileIO.readFully(pageBuf);

            long pageAddr = GridUnsafe.bufferAddress(pageBuf);

            // Remove existing item from index page.
            dataPageIO.removeRow(pageAddr, itemId, pageSize);

            // Recalculate CRC.
            PageIO.setCrc(pageAddr, 0);

            pageBuf.rewind();
            PageIO.setCrc(pageAddr, FastCrc.calcCrc(pageBuf, pageSize));

            // Write it back.
            pageBuf.rewind();
            fileIO.position(pageOff);
            fileIO.writeFully(pageBuf);
        }

        srv = startGrid(0);

        // Add modified page to WAL so it won't be restored to previous (valid) state.
        pageBuf.rewind();
        ByteBuffer cpBuf = ByteBuffer.allocate(pageBuf.capacity());
        cpBuf.put(pageBuf);

        PageSnapshot pageSnapshot = new PageSnapshot(new FullPageId(pageId, grpId), cpBuf.array(), pageSize);

        srv.context().cache().context().wal().log(pageSnapshot);

        // Access cache.
        cache = srv.cache(DEFAULT_CACHE_NAME);

        try {
            for (int i = 0; i < CACHE_ENTRIES; i++)
                cache.get(i);

            fail("Cache operations are expected to fail");
        }
        catch (Throwable e) {
            assertTrue(X.hasCause(e, CorruptedTreeException.class));
        }

        assertTrue(GridTestUtils.waitForCondition(() -> G.allGrids().isEmpty(), 10_000L));

        assertTrue(diagnosticDir.exists());
        assertTrue(diagnosticDir.isDirectory());

        File[] txtFiles = diagnosticDir.listFiles((dir, name) -> name.endsWith(".txt"));

        assertTrue(txtFiles != null && txtFiles.length == 1);

        File[] rawFiles = diagnosticDir.listFiles((dir, name) -> name.endsWith(".raw"));

        assertTrue(rawFiles != null && rawFiles.length == 1);
    }

    /** */
    private class CollectLinkFileIOFactory implements FileIOFactory {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private final FileIOFactory delegateFactory = new RandomAccessFileIOFactory();

        /** {@inheritDoc} */
        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            FileIO fileIO = delegateFactory.create(file, modes);

            return new FileIODecorator(fileIO) {
                @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
                    int type = PageIO.getType(srcBuf);

                    AbstractDataLeafIO dataLeafIO = null;

                    if (type == PageIO.T_DATA_REF_LEAF)
                        dataLeafIO = DataLeafIO.VERSIONS.latest();

                    if (type == PageIO.T_DATA_REF_MVCC_LEAF)
                        dataLeafIO = MvccDataLeafIO.VERSIONS.latest();

                    if (dataLeafIO != null) {
                        long pageAddr = GridUnsafe.bufferAddress(srcBuf);

                        int itemIdx = dataLeafIO.getCount(pageAddr) - 1;

                        linkRef.set(dataLeafIO.getLink(pageAddr, itemIdx));

                        fileRef.set(file);
                    }

                    srcBuf.rewind();

                    return super.write(srcBuf, position);
                }
            };
        }
    }
}
