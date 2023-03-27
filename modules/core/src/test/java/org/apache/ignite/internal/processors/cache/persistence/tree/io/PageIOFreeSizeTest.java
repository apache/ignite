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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.cache.query.index.IndexProcessor;
import org.apache.ignite.internal.cache.query.index.sorted.IndexRow;
import org.apache.ignite.internal.cache.query.index.sorted.inline.io.AbstractInlineInnerIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.PagesList;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.freelist.io.PagesListNodeIO;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMetrics;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.util.IgniteUtils.KB;

/** Tests {@link PageIO#getFreeSpace(int, long)} method for different {@link PageIO} implementations. */
@RunWith(Parameterized.class)
public class PageIOFreeSizeTest extends GridCommonAbstractTest {
    /** Page size. */
    @Parameterized.Parameter
    public int pageSz;

    /** */
    @Parameterized.Parameters(name = "pageSz={0}")
    public static Collection<?> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (long pageSz : new long[] {4 * KB, 8 * KB, 16 * KB})
            params.add(new Object[] {(int)pageSz});

        return params;
    }

    /** Page buffer. */
    private ByteBuffer buf;

    /** Page address. */
    private long addr;

    /** */
    private final PageMetrics mock = new PageMetrics() {
        final LongAdderMetric totalPages = new LongAdderMetric("a", null);

        final LongAdderMetric idxPages = new LongAdderMetric("b", null);

        @Override public LongAdderMetric totalPages() {
            return totalPages;
        }

        @Override public LongAdderMetric indexPages() {
            return idxPages;
        }

        @Override public void reset() {
            // No-op.
        }
    };

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        IndexProcessor.registerIO();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        buf = GridUnsafe.allocateBuffer(pageSz);
        addr = GridUnsafe.bufferAddress(buf);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        GridUnsafe.freeBuffer(buf);
    }

    /** */
    @Test
    public void testPagesListMetaIO() {
        PagesListMetaIO io = io(PagesListMetaIO.VERSIONS);

        int emptyPageSz = io.getCapacity(pageSz, addr) * PagesListMetaIO.ITEM_SIZE;

        assertEquals(emptyPageSz, io.getFreeSpace(pageSz, addr));

        io.addTails(pageSz, addr, 1, new PagesList.Stripe[] {new PagesList.Stripe(1, true)}, 0);

        assertEquals(emptyPageSz - PagesListMetaIO.ITEM_SIZE, io.getFreeSpace(pageSz, addr));
    }

    /** */
    @Test
    public void testPagesListNodeIO() {
        PagesListNodeIO io = io(PagesListNodeIO.VERSIONS);

        int emptyPageSz = io.getCapacity(pageSz) * Long.BYTES;

        assertEquals(emptyPageSz, io.getFreeSpace(pageSz, addr));

        io.addPage(addr, 42L, pageSz);
        io.addPage(addr, 43L, pageSz);

        assertEquals(emptyPageSz - 2 * Long.BYTES, io.getFreeSpace(pageSz, addr));
    }

    /** */
    @Test
    public void testTrackingPageIO() {
        assertEquals(0, io(TrackingPageIO.VERSIONS).getFreeSpace(pageSz, addr));
    }

    /** */
    @Test
    public void testPageMetaIO() {
        assertEquals(0, io(PageMetaIO.VERSIONS).getFreeSpace(pageSz, addr));
        assertEquals(0, io(PageMetaIOV2.VERSIONS).getFreeSpace(pageSz, addr));
    }

    /** */
    @Test
    public void testPartitionCountersIO() {
        PagePartitionCountersIO io = io(PagePartitionCountersIO.VERSIONS);

        int emptyPageSz = io.getCapacity(pageSz) * PagePartitionCountersIO.ITEM_SIZE;

        assertEquals(emptyPageSz, io.getFreeSpace(pageSz, addr));

        io.writeCacheSizes(pageSz, addr, new byte[PagePartitionCountersIO.ITEM_SIZE * 3], 0);

        assertEquals(emptyPageSz - 3 * PagePartitionCountersIO.ITEM_SIZE, io.getFreeSpace(pageSz, addr));
    }

    /** */
    @Test
    public void testBPlusMetaIO() {
        BPlusMetaIO io = io(BPlusMetaIO.VERSIONS);

        int emptyPageSz = io.getMaxLevels(addr, pageSz) * Long.BYTES;

        assertEquals(emptyPageSz, io.getFreeSpace(pageSz, addr));

        io.initRoot(addr, 42L, pageSz);

        assertEquals(emptyPageSz - Long.BYTES, io.getFreeSpace(pageSz, addr));
    }

    /** */
    @Test
    public void testBPlusIO() throws IgniteCheckedException {
        BPlusInnerIO<IndexRow> io = io(AbstractInlineInnerIO.versions(42, false));

        int emptyPageSz = io.getMaxCount(addr, pageSz) * io.getItemSize();

        assertEquals(emptyPageSz, io.getFreeSpace(pageSz, addr));

        io.insert(addr, 0, null, new byte[42], 42, true);

        // Inner pages contains extra link to next level.
        assertEquals(emptyPageSz - (42 + Long.BYTES), io.getFreeSpace(pageSz, addr));
    }

    /** */
    @Test
    public void testDataPageIO() throws IgniteCheckedException {
        DataPageIO io = io(DataPageIO.VERSIONS);

        int emptyPageSz = pageSz - DataPageIO.ITEMS_OFF - DataPageIO.ITEM_SIZE - DataPageIO.PAYLOAD_LEN_SIZE - DataPageIO.LINK_SIZE;

        assertEquals(emptyPageSz, io.getFreeSpace(pageSz, addr));

        io.addRow(addr, new byte[42], pageSz);

        // See AbstractDataPageIO#getPageEntrySize(int, byte)
        assertEquals(emptyPageSz - 42 - 4 /* data size added. */, io.getFreeSpace(pageSz, addr));
    }

    /** */
    private <I extends PageIO> I io(IOVersions<I> versions) {
        I io = versions.latest();

        GridUnsafe.zeroMemory(addr, pageSz);

        io.initNewPage(addr, 1, pageSz, mock);

        return io;
    }
}
