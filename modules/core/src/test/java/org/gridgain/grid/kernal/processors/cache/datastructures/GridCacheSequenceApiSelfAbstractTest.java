/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Cache sequence basic tests.
 */
public abstract class GridCacheSequenceApiSelfAbstractTest extends GridCommonAbstractTest {
    /**  */
    protected static final int BATCH_SIZE = 3;

    /** Number of sequences. */
    protected static final int SEQ_NUM = 3;

    /** Number of loops in method execution. */
    protected static final int MAX_LOOPS_NUM = 1000;

    /** Number of threads for multi-threaded test. */
    protected static final int THREAD_NUM = 10;

    /** Random number generator. */
    protected static final Random RND = new Random();

    /** Names of mandatory sequences. */
    private static String[] seqNames = new String[SEQ_NUM];

    /** Mandatory sequences. */
    private static GridCacheAtomicSequence[] seqArr = new GridCacheAtomicSequence[SEQ_NUM];

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /**
     * Constructor.
     */
    protected GridCacheSequenceApiSelfAbstractTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        IgniteConfiguration cfg = super.getConfiguration();

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        cfg.setLocalHost("127.0.0.1");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        //Prepare names of mandatory sequences.
        for (int i = 0; i < SEQ_NUM; i++)
            seqNames[i] = UUID.randomUUID().toString();

        // Prepare mandatory sequences.
        seqArr[0] = grid().cache(null).dataStructures().atomicSequence(seqNames[0], 0, true);
        seqArr[1] = grid().cache(null).dataStructures().atomicSequence(seqNames[1], RND.nextLong(), true);
        seqArr[2] = grid().cache(null).dataStructures().atomicSequence(seqNames[2], -1 * RND.nextLong(), true);

        // Check and change batch size.
        for (GridCacheAtomicSequence seq : seqArr) {
            assert seq != null;

            // Compare with default batch size.
            assert BATCH_SIZE == seq.batchSize();
        }

        assertEquals(1, G.allGrids().size());
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        // Remove mandatory sequences from cache.
        for (String seqName : seqNames) {
            assert grid().cache(null).dataStructures().removeAtomicSequence(seqName) : seqName;

            assert !grid().cache(null).dataStructures().removeAtomicSequence(seqName) : seqName;
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPrepareSequence() throws Exception {
        // Random sequence names.
        String locSeqName1 = UUID.randomUUID().toString();
        String locSeqName2 = UUID.randomUUID().toString();

        GridCacheAtomicSequence locSeq1 = grid().cache(null).dataStructures().atomicSequence(locSeqName1, 0, true);
        GridCacheAtomicSequence locSeq2 = grid().cache(null).dataStructures().atomicSequence(locSeqName2, 0, true);
        GridCacheAtomicSequence locSeq3 = grid().cache(null).dataStructures().atomicSequence(locSeqName1, 0, true);

        assertNotNull(locSeq1);
        assertNotNull(locSeq2);
        assertNotNull(locSeq3);

        assert locSeq1.equals(locSeq3);
        assert locSeq3.equals(locSeq1);
        assert !locSeq3.equals(locSeq2);

        assert grid().cache(null).dataStructures().removeAtomicSequence(locSeqName1);
        assert grid().cache(null).dataStructures().removeAtomicSequence(locSeqName2);
        assert !grid().cache(null).dataStructures().removeAtomicSequence(locSeqName1);
        assert !grid().cache(null).dataStructures().removeAtomicSequence(locSeqName2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddWrongValue() throws Exception {
        for (GridCacheAtomicSequence seq : seqArr) {
            try {
                seq.getAndAdd(-15);

                fail("Exception expected.");
            }
            catch (IllegalArgumentException e) {
                info("Caught expected exception: " + e);
            }

            try {
                seq.addAndGet(-15);

                fail("Exception expected.");
            }
            catch (IllegalArgumentException e) {
                info("Caught expected exception: " + e);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndIncrement() throws Exception {
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            for (GridCacheAtomicSequence seq : seqArr)
                getAndIncrement(seq);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIncrementAndGet() throws Exception {
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            for (GridCacheAtomicSequence seq : seqArr)
                incrementAndGet(seq);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testAddAndGet() throws Exception {
        for (int i = 1; i < MAX_LOOPS_NUM; i++) {
            for (GridCacheAtomicSequence seq : seqArr)
                addAndGet(seq, i);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndAdd() throws Exception {
        for (int i = 1; i < MAX_LOOPS_NUM; i++) {
            for (GridCacheAtomicSequence seq : seqArr)
                getAndAdd(seq, i);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetAndAddInTx() throws Exception {
        try (GridCacheTx tx = grid().cache(null).txStart(PESSIMISTIC, REPEATABLE_READ)) {
            for (int i = 1; i < MAX_LOOPS_NUM; i++) {
                for (GridCacheAtomicSequence seq : seqArr)
                    getAndAdd(seq, i);

                if (i % 100 == 0)
                    info("Finished iteration: " + i);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testSequenceIntegrity0() throws Exception {
        // Random sequence names.
        String locSeqName1 = UUID.randomUUID().toString();
        String locSeqName2 = UUID.randomUUID().toString();

        // Sequence.
        GridCacheAtomicSequence locSeq1 = grid().cache(null).dataStructures().atomicSequence(locSeqName1, 0, true);

        locSeq1.batchSize(1);

        // Sequence.
        long initVal = -1500;

        GridCacheAtomicSequence locSeq2 = grid().cache(null).dataStructures().atomicSequence(locSeqName2, initVal, true);

        locSeq2.batchSize(7);

        // Compute sequence value manually and compare with sequence value.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            integrity(locSeq1, i * 4);
            integrity(locSeq2, (i * 4) + initVal);

            if (i % 100 == 0)
                info("Finished iteration: " + i);
        }

        assert grid().cache(null).dataStructures().removeAtomicSequence(locSeqName1);
        assert grid().cache(null).dataStructures().removeAtomicSequence(locSeqName2);
        assert !grid().cache(null).dataStructures().removeAtomicSequence(locSeqName1);
        assert !grid().cache(null).dataStructures().removeAtomicSequence(locSeqName2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSequenceIntegrity1() throws Exception {
        sequenceIntegrity(1, 0);
        sequenceIntegrity(7, -1500);
        sequenceIntegrity(3, 345);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiThreadedSequenceIntegrity() throws Exception {
        multiThreadedSequenceIntegrity(1, 0);
        multiThreadedSequenceIntegrity(7, -1500);
        multiThreadedSequenceIntegrity(3, 345);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEviction() throws Exception {
        String locSeqName = UUID.randomUUID().toString();

        GridCacheAtomicSequence locSeq = grid().cache(null).dataStructures().atomicSequence(locSeqName, 0, true);

        locSeq.addAndGet(153);

        grid().cache(null).evictAll();

        assert null != grid().cache(null).get(new GridCacheInternalKeyImpl(locSeqName));
    }

    /**
     * @throws Exception If failed.
     */
    public void testRemove() throws Exception {
        String locSeqName = UUID.randomUUID().toString();

        GridCacheAtomicSequence seq = grid().cache(null).dataStructures().atomicSequence(locSeqName, 0, true);

        seq.addAndGet(153);

        grid().cache(null).dataStructures().removeAtomicSequence(locSeqName);

        try {
            seq.addAndGet(153);

            fail("Exception expected.");
        }
        catch (GridException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheSets() throws Exception {
        // Make new atomic sequence in cache.
        GridCacheAtomicSequence seq = grid().cache(null).dataStructures()
            .atomicSequence(UUID.randomUUID().toString(), 0, true);

        seq.incrementAndGet();

        for (Object o : grid().cache(null).keySet())
            assert !(o instanceof GridCacheInternal) : "Wrong keys [key=" + o + ", keySet=" + grid().cache(null).keySet() +
                ']';

        for (Object o : grid().cache(null).values())
            assert !(o instanceof GridCacheInternal) : "Wrong values [value=" + o + ", values=" +
                grid().cache(null).values() + ']';

        for (Object o : grid().cache(null).entrySet())
            assert !(o instanceof GridCacheInternal) : "Wrong entries [entry=" + o + ", entries=" +
                grid().cache(null).values() + ']';

        assert grid().cache(null).keySet().isEmpty();

        assert grid().cache(null).values().isEmpty();

        assert grid().cache(null).entrySet().isEmpty();

        assert grid().cache(null).size() == 0;

        for (String seqName : seqNames)
            assert null != grid().cache(null).get(new GridCacheInternalKeyImpl(seqName));
    }

    /**
     * Sequence get and increment.
     *
     * @param seq Sequence for test.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long getAndIncrement(GridCacheAtomicSequence seq) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal, seq.getAndIncrement());

        assertEquals(locSeqVal + 1, seq.get());

        return seq.get();
    }

    /**
     * Sequence add and increment
     *
     * @param seq Sequence for test.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long incrementAndGet(GridCacheAtomicSequence seq) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal + 1, seq.incrementAndGet());

        assertEquals(locSeqVal + 1, seq.get());

        return seq.get();
    }

    /**
     * Sequence add and get.
     *
     * @param seq Sequence for test.
     * @param l Number of added elements.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long addAndGet(GridCacheAtomicSequence seq, long l) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal + l, seq.addAndGet(l));

        assertEquals(locSeqVal + l, seq.get());

        return seq.get();
    }

    /**
     * Sequence add and get.
     *
     * @param seq Sequence for test.
     * @param l Number of added elements.
     * @throws Exception If failed.
     * @return Result of operation.
     */
    private long getAndAdd(GridCacheAtomicSequence seq, long l) throws Exception {
        long locSeqVal = seq.get();

        assertEquals(locSeqVal, seq.getAndAdd(l));

        assertEquals(locSeqVal + l, seq.get());

        return seq.get();
    }

    /**
     *  Sequence integrity.
     *
     * @param batchSize Sequence batch size.
     * @param initVal  Sequence initial value.
     * @throws Exception If test fail.
     */
    private void sequenceIntegrity(int batchSize, long initVal) throws Exception {
        // Random sequence names.
        String locSeqName = UUID.randomUUID().toString();

        // Sequence.
        GridCacheAtomicSequence locSeq = grid().cache(null).dataStructures().atomicSequence(locSeqName, initVal, true);

        locSeq.batchSize(batchSize);

        // Result set.
        Collection<Long> resSet = new HashSet<>();

        // Get sequence value and try to put it result set.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            Long val = locSeq.getAndIncrement();

            assert resSet.add(val) : "Element already in set : " + val;
        }

        assert resSet.size() == MAX_LOOPS_NUM;

        for (long i = initVal; i < MAX_LOOPS_NUM + initVal; i++)
            assert resSet.contains(i) : "Element is absent in set : " + i;

        assert grid().cache(null).dataStructures().removeAtomicSequence(locSeqName);
        assert !grid().cache(null).dataStructures().removeAtomicSequence(locSeqName);
    }

    /**
     *  Multi-threaded integrity.
     *
     * @param batchSize Sequence batch size.
     * @param initVal  Sequence initial value.
     * @throws Exception If test fail.
     */
    private void multiThreadedSequenceIntegrity(int batchSize, long initVal) throws Exception {
        // Random sequence names.
        String locSeqName = UUID.randomUUID().toString();

        // Sequence.
        final GridCacheAtomicSequence locSeq = grid().cache(null).dataStructures().atomicSequence(locSeqName, initVal,
            true);

        locSeq.batchSize(batchSize);

        // Result set.
        final Set<Long> resSet = Collections.synchronizedSet(new HashSet<Long>());

        // Get sequence value and try to put it result set.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            Long val = locSeq.getAndIncrement();

            assert !resSet.contains(val) : "Element already in set : " + val;

            resSet.add(val);

            if (i % 100 == 0)
                info("Finished iteration 1: " + i);
        }

        // Work with sequences in many threads.
        multithreaded(
            new Callable() {
                @Nullable @Override public Object call() throws Exception {
                    // Get sequence value and try to put it result set.
                    for (int i = 0; i < MAX_LOOPS_NUM; i++) {
                        Long val = locSeq.getAndIncrement();

                        assert !resSet.contains(val) : "Element already in set : " + val;

                        resSet.add(val);
                    }

                    return null;
                }
            }, THREAD_NUM);

        // Get sequence value and try to put it result set.
        for (int i = 0; i < MAX_LOOPS_NUM; i++) {
            Long val = locSeq.getAndIncrement();

            assert !resSet.contains(val) : "Element already in set : " + val;

            resSet.add(val);


            if (i % 100 == 0)
                info("Finished iteration 2: " + i);
        }

        assert resSet.size() == MAX_LOOPS_NUM * (THREAD_NUM + 2);

        for (long i = initVal; i < MAX_LOOPS_NUM * (THREAD_NUM + 2) + initVal; i++) {
            assert resSet.contains(i) : "Element is absent in set : " + i;

            if (i % 100 == 0)
                info("Finished iteration 3: " + i);
        }

        assert grid().cache(null).dataStructures().removeAtomicSequence(locSeqName);
        assert !grid().cache(null).dataStructures().removeAtomicSequence(locSeqName);
    }

    /**
     * Test sequence integrity.
     *
     * @param seq Sequence for test.
     * @param calcVal Manually calculated value.
     * @throws Exception If failed.
     */
    private void integrity(GridCacheAtomicSequence seq, long calcVal) throws Exception {
        assert calcVal == seq.get();

        getAndAdd(seq, 1);

        assert calcVal + 1 == seq.get();

        addAndGet(seq, 1);

        assert calcVal + 2 == seq.get();

        getAndIncrement(seq);

        assert calcVal + BATCH_SIZE == seq.get();

        incrementAndGet(seq);

        assert calcVal + 4 == seq.get();
    }
}
