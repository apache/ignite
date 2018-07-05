package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.ThreadLocalRandom;


public class SegmentReservationStorageTest extends GridCommonAbstractTest {
    public void test() {
        SegmentReservationStorage segmentReservationStorage = new SegmentReservationStorage();

        int i = ThreadLocalRandom.current().nextInt(1000);
        int idx = ThreadLocalRandom.current().nextInt(1000);

        for (int j = 0; j < i; j++) {
            segmentReservationStorage.reserve(idx);
        }

        for (int j = 0; j < i; j++) {
            assertTrue(segmentReservationStorage.reserved(idx));
            segmentReservationStorage.release(idx);
        }

        assertFalse(segmentReservationStorage.reserved(idx));
    }
}