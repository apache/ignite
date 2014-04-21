/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testsuites;

import junit.framework.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.local.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.partitioned.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.replicated.*;

/**
 * Test suite for cache data structures.
 */
public class GridCacheDataStructuresSelfTestSuite extends TestSuite {
    /**
     * @return Cache test suite.
     * @throws Exception If failed.
     */
    public static TestSuite suite() throws Exception {
        TestSuite suite = new TestSuite("Gridgain Cache Data Structures Test Suite");

        // Data structures.
        suite.addTest(new TestSuite(GridCachePartitionedQueueFailoverDataConsistencySelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueFailoverDataConsistencySelfTest.class));

        suite.addTest(new TestSuite(GridCacheLocalSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicSetSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheLocalAtomicQueueApiSelfTest.class));

        suite.addTest(new TestSuite(GridCacheReplicatedSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedSequenceMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedSetSelfTest.class));
        // TODO: GG-5306
        // suite.addTest(new TestSuite(GridCacheReplicatedDataStructuresFailoverSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedSequenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedSequenceMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueApiSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueCreateMultiNodeSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedSetSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicSetSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedSetFailoverSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicSetFailoverSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedAtomicQueueRotativeMultiNodeTest.class));
        suite.addTest(new TestSuite(GridCacheQueueCleanupSelfTest.class));

        // TODO: GG-5620 Uncomment when fix
        //suite.addTest(new TestSuite(GridCachePartitionedQueueEntryMoveSelfTest.class));

        // TODO: GG-2699
        //suite.addTest(new TestSuite(GridCachePartitionedDataStructuresFailoverSelfTest.class));
        // TODO: GG-4807 Uncomment when fix
        // suite.addTest(new TestSuite(GridCacheQueueMultiNodeConsistencySelfTest.class));

        suite.addTest(new TestSuite(GridCacheCountDownLatchSelfTest.class));
        suite.addTest(new TestSuite(GridCacheAtomicLongApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicSequenceMultiThreadedTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicStampedApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedAtomicStampedApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedAtomicReferenceApiSelfTest.class));
        suite.addTest(new TestSuite(GridCacheReplicatedAtomicReferenceApiSelfTest.class));

        suite.addTest(new TestSuite(GridCachePartitionedNodeRestartTxSelfTest.class));
        suite.addTest(new TestSuite(GridCachePartitionedQueueJoinedNodeSelfTest.class));

        return suite;
    }
}
