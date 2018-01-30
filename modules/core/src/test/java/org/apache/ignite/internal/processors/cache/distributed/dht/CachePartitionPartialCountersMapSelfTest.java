package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.CachePartitionPartialCountersMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

public class CachePartitionPartialCountersMapSelfTest extends GridCommonAbstractTest {

    public void testAddAndRemove() throws Exception {
        CachePartitionPartialCountersMap map = new CachePartitionPartialCountersMap(10);

        for (int p = 0; p < 10; p++)
            map.add(p, 2 * p, 3 * p);

        for (int p = 0; p < 10; p++) {
            assertEquals(p, map.partitionAt(p));
            assertEquals(2 * p, map.initialUpdateCounterAt(p));
            assertEquals(3 * p, map.updateCounterAt(p));
        }

        map.remove(3);
        map.remove(11);
        map.remove(7);

        assertEquals(8, map.size());

        int idx = 0;

        for (int p = 0; p < 10; p++) {
            if (p == 3 || p == 10 || p == 7)
                continue;

            assertEquals(p, map.partitionAt(idx));
            assertEquals(2 * p, map.initialUpdateCounterAt(idx));
            assertEquals(3 * p, map.updateCounterAt(idx));

            idx++;
        }
    }

}