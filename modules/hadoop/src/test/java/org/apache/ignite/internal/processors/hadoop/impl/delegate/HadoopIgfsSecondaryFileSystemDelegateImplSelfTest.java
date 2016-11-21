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

package org.apache.ignite.internal.processors.hadoop.impl.delegate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.internal.processors.igfs.IgfsBlockLocationImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Unit test for the method HadoopIgfsSecondaryFileSystemDelegateImpl.resliceBlocks
 */
public class HadoopIgfsSecondaryFileSystemDelegateImplSelfTest extends GridCommonAbstractTest {
    /** Block size. */
    private static final int BLOCK_SIZE = 4 * 1024;

    /** Group size. */
    private static final int HOSTS_COUNT = 3;

    /** Group size. */
    private static final int FILE_BLOCKS = 32;

    /**
     * @param blocks Blocks count.
     * @param blockSize Block's size.
     * @param hostCnt Hosts count.
     * @param start begin offset
     * @param len length.
     * @return Block array.
     */
    private IgfsBlockLocation[] createBlocks(int blocks, int blockSize, int hostCnt, long start, long len) {
        List<IgfsBlockLocation> blks = new ArrayList<>(blocks);

        for (int i = 0; i < blocks; ++i) {
            if ((i + 1) * blockSize < start)
                continue;

            if (i * blockSize >= start + len)
                break;

            String host = "host_" + i * hostCnt / blocks;

            blks.add(new IgfsBlockLocationImpl(
                i * blockSize,
                blockSize,
                Collections.singleton(host), Collections.singleton(host)));
        }

        blks.set(0, new IgfsBlockLocationImpl(
            start,
            blks.get(0).length() - (start % blockSize),
            blks.get(0)));

        blks.set(blks.size() - 1, new IgfsBlockLocationImpl(
            blks.get(blks.size() - 1).start(),
            ((start % blockSize) + len) % blockSize == 0 ? blockSize : ((start % blockSize) + len) % blockSize,
            blks.get(blks.size() - 1)));

        return blks.toArray(new IgfsBlockLocationImpl[blks.size()]);
    }


    /**
     * @throws Exception If failed.
     */
    public void testIgfsBlockLocationReslice() throws  Exception {
        int testIters = 10;

        long start = 0;
        long len = FILE_BLOCKS * BLOCK_SIZE;
        long maxLen = BLOCK_SIZE * 2;

        long step = BLOCK_SIZE / 4;

        for (int i = 0; i < testIters; ++i) {
            Collection<IgfsBlockLocation> blocks = HadoopIgfsSecondaryFileSystemDelegateImpl.resliceBlocks(
                createBlocks(FILE_BLOCKS, BLOCK_SIZE, HOSTS_COUNT, start, len),
                start,
                len,
                maxLen);

            assertEquals(F.first(blocks).start(), start);
            assertEquals(start + len, F.last(blocks).start() + F.last(blocks).length());

            long totalLen = 0;
            for (IgfsBlockLocation block : blocks) {
                totalLen += block.length();

                assert block.length() <= maxLen : "block.length() <= maxLen. [block.length=" + block.length()
                    + ", maxLen=" + maxLen + ']';

                assert block.length() + block.start() <= start + len : "block.length() + block.start() < start + len. [block.length=" + block.length()
                    + ", block.start()=" + block.start() + ", start=" + start +", len=" + len + ']';

                for (IgfsBlockLocation block0 : blocks)
                    if (!block0.equals(block))
                        assert block.start() < block0.start() && block.start() + block.length() <= block0.start() ||
                            block.start() > block0.start() && block0.start() + block0.length() <= block.start()
                            : "Blocks cross each other: block0=" +  block + ", block1= " + block0;
            }

            assert totalLen == len : "Summary length of blocks must be: " + len + " actual: " + totalLen;

            len -= step * 2;
            start += step;
            maxLen += step;
        }
    }
}