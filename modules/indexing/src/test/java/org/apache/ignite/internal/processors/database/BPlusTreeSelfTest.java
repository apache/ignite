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

package org.apache.ignite.internal.processors.database;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.mem.unsafe.UnsafeMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.impl.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.database.tree.BPlusTree;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusInnerIO;
import org.apache.ignite.internal.processors.cache.database.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class BPlusTreeSelfTest extends GridCommonAbstractTest {
    /** */
    private static final short LONG_INNER_IO = 30000;

    /** */
    private static final short LONG_LEAF_IO = 30001;

    /** */
    private static final int PAGE_SIZE = 256;

    /** */
    private static final long MB = 1024 * 1024;

    /** */
    private static final int CPUS = Runtime.getRuntime().availableProcessors();

    /** */
    private static final int CACHE_ID = 100500;

    /** */
    private PageMemory pageMem;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        pageMem = new PageMemoryImpl(log, new UnsafeMemoryProvider(64 * MB, 32 * MB), null, PAGE_SIZE, CPUS);

        pageMem.start();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        pageMem.stop();
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testPutFindRemoveForward0() throws IgniteCheckedException {
        doTestPutFindRemoveForward(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testPutFindRemoveForward1() throws IgniteCheckedException {
        doTestPutFindRemoveForward(true);
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestPutFindRemoveForward(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = createTestTree(canGetRow);

        long cnt = 100_000;

        for (long x = 0; x < cnt; x++) {
            assertNull(tree.findOne(x));

            tree.put(x);

            assertEquals(x, tree.findOne(x).longValue());
        }

        assertNull(tree.findOne(-1L));

        for (long x = 0; x < cnt; x++)
            assertEquals(x, tree.findOne(x).longValue());

        assertNull(tree.findOne(cnt));

        for (long x = 0; x < cnt; x++) {
            assertEquals(x, tree.remove(x).longValue());

            assertNull(tree.findOne(x));
        }

        assertFalse(tree.find(null, null).next());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testPutRemoveFindBackward0() throws IgniteCheckedException {
        doTestPutFindRemoveBackward(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testPutRemoveFindBackward1() throws IgniteCheckedException {
        doTestPutFindRemoveBackward(true);
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestPutFindRemoveBackward(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = createTestTree(canGetRow);

        long cnt = 100_000;

        for (long x = cnt - 1; x >= 0; x--) {
            assertNull(tree.findOne(x));

            tree.put(x);

            assertEquals(x, tree.findOne(x).longValue());
        }

        assertNull(tree.findOne(cnt));

        for (long x = cnt - 1; x >= 0; x--)
            assertEquals(x, tree.findOne(x).longValue());

        assertNull(tree.findOne(-1L));

        for (long x = cnt - 1; x >= 0; x--) {
            assertEquals(x, tree.remove(x).longValue());

            assertNull(tree.findOne(x));
        }

        assertFalse(tree.find(null, null).next());
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testPutRemoveFindRandom0() throws IgniteCheckedException {
        doTestPutRemoveFindRandom(false);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    public void testPutRemoveFindRandom1() throws IgniteCheckedException {
        doTestPutRemoveFindRandom(true);
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @throws IgniteCheckedException If failed.
     */
    private void doTestPutRemoveFindRandom(boolean canGetRow) throws IgniteCheckedException {
        TestTree tree = createTestTree(canGetRow);
        Map<Long,Long> map = new HashMap<>();

        long seed = System.currentTimeMillis();

        X.println("seed: " + seed);

        Random rnd = new GridRandom(seed);

        int cnt = 100_000;

        for (int i = 0, end = 30 * cnt; i < end; i++) {
            long x = rnd.nextInt(cnt);

            switch(rnd.nextInt(3)) {
                case 0:
                    assertEquals(map.put(x, x), tree.put(x));

                case 1:
                    assertEquals(map.get(x), tree.findOne(x));

                    break;

                case 2:
                    assertEquals(map.remove(x), tree.remove(x));

                    break;

                default:
                    fail();
            }
        }

        assertFalse(map.isEmpty());

        for (Long x : map.keySet())
            assertEquals(map.get(x), tree.findOne(x));

        GridCursor<Long> cursor = tree.find(null, null);

        while(cursor.next()) {
            Long x = cursor.get();

            assert x != null;

            assertEquals(map.remove(x), x);
        }

        assertTrue(map.isEmpty());
    }

    /**
     * @param canGetRow Can get row from inner page.
     * @return Test tree instance.
     * @throws IgniteCheckedException If failed.
     */
    private TestTree createTestTree(boolean canGetRow) throws IgniteCheckedException {
        return new TestTree(canGetRow, CACHE_ID, pageMem, allocatePage());
    }

    /**
     * @return Allocated full page ID.
     * @throws IgniteCheckedException If failed.
     */
    private FullPageId allocatePage() throws IgniteCheckedException {
        return pageMem.allocatePage(CACHE_ID, -1, PageIdAllocator.FLAG_IDX);
    }

    /**
     * Test tree.
     */
    private static class TestTree extends BPlusTree<Long, Long> {
        /** */
        final boolean canGetRow;

        /**
         * @param canGetRow Can get row from inner page.
         * @param cacheId Cache ID.
         * @param pageMem Page memory.
         * @param metaPageId Meta page ID.
         * @throws IgniteCheckedException If failed.
         */
        public TestTree(boolean canGetRow, int cacheId, PageMemory pageMem, FullPageId metaPageId)
            throws IgniteCheckedException {
            super(cacheId, pageMem, metaPageId);

            this.canGetRow = canGetRow;

            initNew();
        }

        /** {@inheritDoc} */
        @Override protected BPlusIO<Long> io(int type, int ver) {
            BPlusIO<Long> io = io(type);

            assert io.getVersion() == ver: ver;

            return io;
        }

        /**
         * @param type Type.
         * @return IO.
         */
        private BPlusIO<Long> io(int type) {
            switch (type) {
                case LONG_INNER_IO:
                    return latestInnerIO();

                case LONG_LEAF_IO:
                    return latestLeafIO();

                default:
                    throw new IllegalStateException("type: " + type);
            }
        }

        /** {@inheritDoc} */
        @Override protected BPlusInnerIO<Long> latestInnerIO() {
            return canGetRow ? LongInnerIO.INSTANCE1 : LongInnerIO.INSTANCE0;
        }

        /** {@inheritDoc} */
        @Override protected BPlusLeafIO<Long> latestLeafIO() {
            return LongLeafIO.INSTANCE;
        }

        /** {@inheritDoc} */
        @Override protected int compare(BPlusIO<Long> io, ByteBuffer buf, int idx, Long n2)
            throws IgniteCheckedException {
            Long n1 = io.getLookupRow(this, buf, idx);

            return Long.compare(n1, n2);
        }

        /** {@inheritDoc} */
        @Override protected Long getRow(BPlusIO<Long> io, ByteBuffer buf, int idx) throws IgniteCheckedException {
            assert io.canGetRow() : io;

            return io.getLookupRow(this, buf, idx);
        }
    }

    /**
     * Long inner.
     */
    private static final class LongInnerIO extends BPlusInnerIO<Long> {
        /** */
        static final LongInnerIO INSTANCE0 = new LongInnerIO(false);

        /** */
        static final LongInnerIO INSTANCE1 = new LongInnerIO(true);

        /**
         */
        protected LongInnerIO(boolean canGetRow) {
            super(LONG_INNER_IO, 302, canGetRow, 8);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer dst, int dstIdx, BPlusIO<Long> srcIo, ByteBuffer src, int srcIdx)
            throws IgniteCheckedException {
            store(dst, dstIdx, srcIo.getLookupRow(null, src, srcIdx));
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx, Long row) {
            buf.putLong(offset(idx), row);
        }

        /** {@inheritDoc} */
        @Override public Long getLookupRow(BPlusTree<Long,?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            return buf.getLong(offset(idx));
        }
    }

    /**
     * Long leaf.
     */
    private static final class LongLeafIO extends BPlusLeafIO<Long> {
        /** */
        static final LongLeafIO INSTANCE = new LongLeafIO();

        /**
         */
        protected LongLeafIO() {
            super(LONG_LEAF_IO, 603, 8);
        }

        /** {@inheritDoc} */
        @Override public void store(ByteBuffer buf, int idx, Long row) {
            buf.putLong(offset(idx), row);
        }

        /** {@inheritDoc} */
        @Override public Long getLookupRow(BPlusTree<Long,?> tree, ByteBuffer buf, int idx)
            throws IgniteCheckedException {
            return buf.getLong(offset(idx));
        }
    }
}
