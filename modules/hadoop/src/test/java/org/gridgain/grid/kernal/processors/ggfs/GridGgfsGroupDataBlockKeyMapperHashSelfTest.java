/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.ggfs;

import org.gridgain.grid.*;
import org.gridgain.grid.ggfs.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

/**
 * Tests for {@link GridGgfsGroupDataBlocksKeyMapper} hash.
 */
public class GridGgfsGroupDataBlockKeyMapperHashSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testDistributionGroupSizePower2PartitionCountPower2() throws Exception {
        checkDistribution(256, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionGroupSizePower2PartitionCountNonPower2() throws Exception {
        checkDistribution(256, 63);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionGroupSizeNonPower2PartitionCountPower2() throws Exception {
        checkDistribution(255, 64);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDistributionGroupSizeNonPower2PartitionCountNonPower2() throws Exception {
        checkDistribution(255, 63);
    }

    /**
     * Test distribution for integer overflow.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("NumericOverflow")
    public void testIntOverflowDistribution() throws Exception {
        int partCnt = 100;

        GridUuid fileId = GridUuid.randomUuid();

        GridGgfsGroupDataBlocksKeyMapper mapper = new GridGgfsGroupDataBlocksKeyMapper(1);

        Integer part1 = (Integer) mapper.affinityKey(new GridGgfsBlockKey(fileId, null, false,
            Integer.MAX_VALUE - 1)) % partCnt;
        Integer part2 = (Integer) mapper.affinityKey(new GridGgfsBlockKey(fileId, null, false,
            Integer.MAX_VALUE)) % partCnt;
        Integer part3 = (Integer) mapper.affinityKey(new GridGgfsBlockKey(fileId, null, false,
            (long)Integer.MAX_VALUE + 1)) % partCnt;

        assert U.safeAbs(part1 - part2) == 1;
        assert U.safeAbs(part2 - part3) == 1;
    }

    /**
     * Check hash code generation for the given group size and partitions count.
     *
     * @throws Exception If failed.
     */
    public void checkDistribution(int grpSize, int partCnt) throws Exception {
        GridUuid fileId = GridUuid.randomUuid();

        GridGgfsGroupDataBlocksKeyMapper mapper = new GridGgfsGroupDataBlocksKeyMapper(grpSize);

        int lastPart = 0;

        boolean first = true;

        for (int i = 0; i < 10; i++) {
            // Ensure that all blocks within the group has the same hash codes.
            boolean firstInGroup = true;

            for (int j = 0; j < grpSize; j++) {
                GridGgfsBlockKey key = new GridGgfsBlockKey(fileId, null, false, i * grpSize + j);

                Integer part = (Integer) mapper.affinityKey(key) % partCnt;

                if (firstInGroup) {
                    if (first)
                        first = false;
                    else
                        assert U.safeAbs(lastPart - part) == 1;

                    firstInGroup = false;
                } else
                    assert part == lastPart;

                lastPart = part;
            }
        }
    }
}

