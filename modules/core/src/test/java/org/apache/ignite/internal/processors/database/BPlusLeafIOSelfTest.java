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
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusLeafIO;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

/** Tests methods of {@link BPlusLeafIO} */
public class BPlusLeafIOSelfTest extends GridCommonAbstractTest {
    /** */
    public static final int PAGE_SZ = (int)(4 * U.KB);

    /** Test {@link BPlusLeafIO#remove(long, int[], int)} */
    @Test
    public void testRemoveSeveralElements() throws Exception {
        doTestRemoval(new int[] {0}, new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
        doTestRemoval(new int[] {0, 1}, new long[] {2, 3, 4, 5, 6, 7, 8, 9});
        doTestRemoval(new int[] {9}, new long[] {0, 1, 2, 3, 4, 5, 6, 7, 8});
        doTestRemoval(new int[] {8, 9}, new long[] {0, 1, 2, 3, 4, 5, 6, 7});
        doTestRemoval(new int[] {0, 2, 4}, new long[] {1, 3, 5, 6, 7, 8, 9});
        doTestRemoval(new int[] {0, 2, 3, 4, 5}, new long[] {1, 6, 7, 8, 9});
        doTestRemoval(new int[] {0, 2, 3, 4, 5}, new long[] {1, 6, 7, 8, 9});
        doTestRemoval(new int[] {0, 2, 3, 5}, new long[] {1, 4, 6, 7, 8, 9});
        doTestRemoval(new int[] {0, 2, 4, 9}, new long[] {1, 3, 5, 6, 7, 8});
        doTestRemoval(new int[] {0, 2, 4, 8, 9}, new long[] {1, 3, 5, 6, 7});
        doTestRemoval(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, U.EMPTY_LONGS);

        doTestRemoval(
            new int[] {0, 2, 3, 4, 5, /* Elements after must not be removed because of idxsCnt */ 6, 7, 8, 9},
            5,
            new long[] {1, 6, 7, 8, 9}
        );
    }

    /** */
    private void doTestRemoval(int[] rmvIdxs, long[] expRes) throws IgniteCheckedException {
        doTestRemoval(rmvIdxs, rmvIdxs.length, expRes);
    }

    /** */
    private void doTestRemoval(int[] rmvIdxs, int idxsCnt, long[] expRes) throws IgniteCheckedException {
        ByteBuffer page = ByteBuffer.allocateDirect(PAGE_SZ);
        long pageAddr = bufferAddress(page);
        long pageId = PageIdUtils.pageId(PageIdAllocator.MAX_PARTITION_ID, PageIdAllocator.FLAG_DATA, 171717);

        BPlusTreeSelfTest.LongLeafIO io = new BPlusTreeSelfTest.LongLeafIO();

        io.initNewPage(pageAddr, pageId, PAGE_SZ, null);

        for (int i = 0; i < 10; i++)
            io.store(pageAddr, i, (long)i, null, false);

        io.setCount(pageAddr, 10);

        io.remove(pageAddr, rmvIdxs, idxsCnt);

        assertEquals(io.getCount(pageAddr), expRes.length);

        for (int i = 0; i < expRes.length; i++)
            assertEquals("Element at " + i + " index", expRes[i], (long)io.getLookupRow(null, pageAddr, i));
    }
}
