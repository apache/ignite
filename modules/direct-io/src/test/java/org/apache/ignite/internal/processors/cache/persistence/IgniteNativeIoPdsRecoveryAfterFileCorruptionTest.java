package org.apache.ignite.internal.processors.cache.persistence;

/**
 * Native IO tests can't use page store, so this test limit pages count to 128.
 * 524288 bytes to be read 128 times, 64Mbytes total read load.
 */
public class IgniteNativeIoPdsRecoveryAfterFileCorruptionTest extends IgnitePdsRecoveryAfterFileCorruptionTest {
    /** {@inheritDoc} */
    @Override protected int getTotalPagesToTest() {
        return 128;
    }
}
