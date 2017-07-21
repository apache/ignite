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

package org.apache.ignite.internal.processors.igfs;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Tests for {@link org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper} hash.
 */
public class IgfsGroupDataBlockKeyMapperHashSelfTest extends IgfsCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testDistribution() throws Exception {
        for (int i = 0; i < 100; i++) {
            int grpSize = ThreadLocalRandom.current().nextInt(2, 100000);
            int partCnt = ThreadLocalRandom.current().nextInt(1, grpSize);

            checkDistribution(grpSize, partCnt);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIntOverflowDistribution() throws Exception {
        for (int i = 0; i < 100; i++)
            checkIntOverflowDistribution(ThreadLocalRandom.current().nextInt(1, Integer.MAX_VALUE));
    }

    /**
     * @param mapper IGFS blocks mapper.
     * @param fileId IGFS file ID.
     * @param blockId File block ID.
     * @param partCnt Total partitions count.
     * @return Partition index.
     */
    private int partition(IgfsGroupDataBlocksKeyMapper mapper, IgniteUuid fileId, long blockId, int partCnt) {
        return U.safeAbs((Integer) mapper.affinityKey(new IgfsBlockKey(fileId, null, false, blockId)) % partCnt);
    }

    /**
     * Check hash code generation for the given group size and partitions count.
     *
     * @throws Exception If failed.
     */
    public void checkDistribution(int grpSize, int partCnt) throws Exception {
        IgniteUuid fileId = IgniteUuid.randomUuid();

        IgfsGroupDataBlocksKeyMapper mapper = new IgfsGroupDataBlocksKeyMapper(grpSize);

        int lastPart = 0;

        boolean first = true;

        for (int i = 0; i < 10; i++) {
            // Ensure that all blocks within the group has the same hash codes.
            boolean firstInGroup = true;

            for (int j = 0; j < grpSize; j++) {
                int part = partition(mapper, fileId, i * grpSize + j, partCnt);

                if (firstInGroup) {
                    if (first)
                        first = false;
                    else
                        assert checkPartition(lastPart, part, partCnt) :
                            "[fileId = " + fileId + ", i=" + i + ", j=" + j + ", grpSize= " + grpSize +
                                ", partCnt=" + partCnt + ", lastPart=" + lastPart + ", part=" + part + ']';

                    firstInGroup = false;
                }
                else
                    assert part == lastPart;

                lastPart = part;
            }
        }
    }

    /**
     * Check distribution for integer overflow.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("NumericOverflow")
    public void checkIntOverflowDistribution(int partCnt) throws Exception {
        IgniteUuid fileId = IgniteUuid.randomUuid();

        IgfsGroupDataBlocksKeyMapper mapper = new IgfsGroupDataBlocksKeyMapper(1);

        int part1 = partition(mapper, fileId, Integer.MAX_VALUE - 1, partCnt);
        int part2 = partition(mapper, fileId, Integer.MAX_VALUE, partCnt);
        int part3 = partition(mapper, fileId, (long)Integer.MAX_VALUE + 1, partCnt);

        assert checkPartition(part1, part2, partCnt) :
            "[fileId = " + fileId + "part1=" + part1 + ", part2=" + part2 + ", partCnt=" + partCnt + ']';

        assert checkPartition(part2, part3, partCnt) :
            "[fileId = " + fileId + "part1=" + part2 + ", part3=" + part3 + ", partCnt=" + partCnt + ']';
    }

    /**
     * Check correct partition shift.
     *
     * @param prevPart Previous partition.
     * @param part Current partition.
     * @param totalParts Total partitions.
     * @return {@code true} if previous and current partitions have correct values.
     */
    private boolean checkPartition(int prevPart, int part, int totalParts) {
        return U.safeAbs(prevPart - part) == 1 ||
            (part == 0 && prevPart == totalParts - 1) ||
            (prevPart == 0 && part == totalParts - 1);
    }
}
