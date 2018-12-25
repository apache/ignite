package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.CheckpointProgress;
import org.apache.ignite.internal.processors.cache.persistence.file.AsyncFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODecorator;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;

@RunWith(JUnit4.class)
public class IgnitePdsCheckpointCancelTest extends GridCommonAbstractTest {
    private static final String NODE_CONST_ID = "NODE";
    private static final String REGION_NAME = "MY_REGION";

    private static final int PAGE_SIZE = 4096;
    private static final int REGION_SIZE = 400 * 1024 * 1024;
    private static final int CP_BUFFER_SIZE = 400 * 1024 * 1024;

    private static final int DIRTY_PAGES_BOUND = REGION_SIZE / 2;

    private static final int MIDDLE_PAGE_COUNT = DIRTY_PAGES_BOUND / PAGE_SIZE / 4;

    private List<TestFileIO> partsFilesIO = new ArrayList<>();

    private FileIOFactory ioFactory;

    private boolean cancelCp = true;

    private int partitions;

    private final TimeTracker time = new TimeTracker();

    @Override
    protected IgniteConfiguration getConfiguration(String name) throws Exception {
        assert partitions != 0;

        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setConsistentId(NODE_CONST_ID);

        cfg.setDataStorageConfiguration(
            new DataStorageConfiguration()
                .setFileIOFactory(ioFactory != null ? ioFactory : new AsyncFileIOFactory())
                .setCheckpointFrequency(60 * 60 * 1000)
                .setWalSegments(10)
                .setWalSegmentSize(REGION_SIZE)
                .setMaxWalArchiveSize(REGION_SIZE * 10L)
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setName(REGION_NAME)
                        .setMaxSize(REGION_SIZE)
                        .setCheckpointPageBufferSize(CP_BUFFER_SIZE)
                        .setMetricsEnabled(true)
                        .setPersistenceEnabled(true)
                )
        );

        cfg.setCacheConfiguration(
            new CacheConfiguration(DEFAULT_CACHE_NAME)
                .setDataRegionName(REGION_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, partitions))
        );

        return cfg;
    }

    @Before
    public void before() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

    }

    @After
    public void after() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    public void testCancelOnMarkCheckpointBegin() {
        //TODO
    }

    @Test
    public void testCancelOnPageWrite() throws Exception {
        partitions = 1;
        ioFactory = new TestFileIOFactory(new AsyncFileIOFactory(), partsFilesIO);

        String firstCpTime = "firstCpTime";
        String awaitMiddleWritePagesInCheckpoint = "awaitMiddleWritePagesInCheckpoint";
        String secondCpAwaitBegin = "secondCpAwaitBeginTime";

        Ignite ig = startGrid(0);

        ig.cluster().active(true);

        int key = 0;

        try (IgniteDataStreamer<Integer, byte[]> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);
            st.perNodeBufferSize(10);
            st.perThreadBufferSize(10);

            long allocatedPages;

            for (; ; ) {
                allocatedPages = ig.dataRegionMetrics(REGION_NAME).getTotalAllocatedPages();

                if (allocatedPages >= (MIDDLE_PAGE_COUNT * 3))
                    break;

                st.addData(key++, new byte[PAGE_SIZE]);
            }

            log.info("Added " + key + " key value pairs, allocatedPages=" +
                allocatedPages + ", bound=" + (MIDDLE_PAGE_COUNT * 3));
        }

        log.info("\nConfig: \n" +
            "PAGES_IN_NEXT_CP:" + ig.dataRegionMetrics(REGION_NAME).getTotalAllocatedPages() + "\n" +
            "REGION_SIZE:" + REGION_SIZE + "(" + (REGION_SIZE / PAGE_SIZE) + ")\n" +
            "DIRTY_PAGES_BOUND:" + DIRTY_PAGES_BOUND + "(" + (DIRTY_PAGES_BOUND / PAGE_SIZE) + ")\n" +
            "MIDDLE_PAGE_COUNT:" + MIDDLE_PAGE_COUNT + "\n"
        );

        GridCacheDatabaseSharedManager dbMgr = dbManager(ig);

        TestFileIO.reset();

        CheckpointFuture cp1Future = dbMgr.forceCheckpoint("chp=1", cancelCp);

        cp1Future.beginFuture().get();

        time.begin(firstCpTime);

        cp1Future.finishFuture()
            .listen(f -> {
                time.end(firstCpTime);

                log.info("chp=1 time duration (" + time.getByName(firstCpTime) + ")");
            });

        time.begin(awaitMiddleWritePagesInCheckpoint);

        TestFileIO.await(MIDDLE_PAGE_COUNT);

        time.end(awaitMiddleWritePagesInCheckpoint);

        log.info("Done to wait middle page write in current checkpoint, pages:" +
            TestFileIO.SHARED_PAGE_COUNTER.get());

        Assert.assertTrue(!cp1Future.finishFuture().isDone());

        time.begin(secondCpAwaitBegin);

        dbMgr.forceCheckpoint("chp=2, chp=1 canceled", cancelCp).beginFuture().get();

        time.end(secondCpAwaitBegin);

        long secondCpAwaitBeginTime = time.getByName(secondCpAwaitBegin);
        long awaitMiddleWritePagesInCheckpointTime = time.getByName(awaitMiddleWritePagesInCheckpoint);

        String msg = "waitNextCpTime=" + secondCpAwaitBeginTime +
            ", writePagesInTheMiddleTime=" + awaitMiddleWritePagesInCheckpointTime;

        Assert.assertTrue(msg, secondCpAwaitBeginTime < awaitMiddleWritePagesInCheckpointTime);

        log.info(msg);
    }

    @Test
    public void testCancelOnSyncStores() throws Exception {
        partitions = 1024;

        String firstCpTimeLable = "firstCpTime";
        String secondCpAwaitBeginLabel = "secondCpAwaitBeginTime";

        Ignite ig = startGrid(0);

        ig.cluster().active(true);

        int key = 0;

        try (IgniteDataStreamer<Integer, byte[]> st = ig.dataStreamer(DEFAULT_CACHE_NAME)) {
            st.allowOverwrite(true);
            st.perNodeBufferSize(10);
            st.perThreadBufferSize(10);

            long allocatedPages;

            for (; ; ) {
                allocatedPages = ig.dataRegionMetrics(REGION_NAME).getTotalAllocatedPages();

                if (allocatedPages >= (MIDDLE_PAGE_COUNT * 3))
                    break;

                st.addData(key++, new byte[PAGE_SIZE]);
            }

            log.info("Added " + key + " key value pairs, allocatedPages=" +
                allocatedPages + ", bound=" + (MIDDLE_PAGE_COUNT * 3));
        }

        log.info("\nConfig: \n" +
            "PAGES_IN_NEXT_CP:" + ig.dataRegionMetrics(REGION_NAME).getTotalAllocatedPages() + "\n" +
            "REGION_SIZE:" + REGION_SIZE + "(" + (REGION_SIZE / PAGE_SIZE) + ")\n" +
            "DIRTY_PAGES_BOUND:" + DIRTY_PAGES_BOUND + "(" + (DIRTY_PAGES_BOUND / PAGE_SIZE) + ")\n" +
            "MIDDLE_PAGE_COUNT:" + MIDDLE_PAGE_COUNT + "\n"
        );

        GridCacheDatabaseSharedManager dbMgr = dbManager(ig);

        GridCacheDatabaseSharedManager.Checkpointer checkpointer = U.field(dbMgr, "checkpointer");

        Function<GridCacheDatabaseSharedManager.Checkpoint, Boolean> prevFunc = checkpointer.checkCancel;

        CountDownLatch awaitSyncStores = new CountDownLatch(1);
        CountDownLatch awaitContinueSyncStores = new CountDownLatch(1);

        AtomicBoolean readyToSync = new AtomicBoolean();

        AtomicLong lastCheckTime = new AtomicLong();
        AtomicLong maxCheckCancelTime = new AtomicLong();

        checkpointer.checkCancel = (chp) -> {
            CheckpointProgress cpProgress = chp.checkpointProgress();

            long last = lastCheckTime.getAndSet(System.currentTimeMillis());

            if (last != 0) {
                long lastCheck = System.currentTimeMillis() - last;

                if (lastCheck > maxCheckCancelTime.get())
                    maxCheckCancelTime.set(lastCheck);
            }

            int stores = cpProgress.pageStores().size();

            // All pages written to store and collection size is equals to partitions.
            if (stores == partitions)
                readyToSync.set(true);

            try {
                U.sleep(5);
            }
            catch (IgniteInterruptedCheckedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteException(e);
            }

            if (readyToSync.get()) {
                if (stores < partitions / 10) {
                    readyToSync.set(false);

                    awaitSyncStores.countDown();

                    try {
                        log.info("Done to wait store syncs in current checkpoint, stores:" + stores);

                        awaitContinueSyncStores.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();

                        throw new IgniteException(e);
                    }
                }
            }

            return prevFunc.apply(chp);
        };

        CheckpointFuture cp1Future = dbMgr.forceCheckpoint("chp=1", cancelCp);

        cp1Future.beginFuture().get();

        time.begin(firstCpTimeLable);

        cp1Future.finishFuture()
            .listen(f -> {
                time.end(firstCpTimeLable);

                log.info("chp=1 time duration (" + time.getByName(firstCpTimeLable) + ")");
            });

        awaitSyncStores.await();

        Assert.assertTrue(!cp1Future.finishFuture().isDone());

        time.begin(secondCpAwaitBeginLabel);

        CheckpointFuture chpFuture = dbMgr.forceCheckpoint("chp=2, chp=1 canceled", cancelCp);

        awaitContinueSyncStores.countDown();

        chpFuture.beginFuture().get();

        time.end(secondCpAwaitBeginLabel);

        chpFuture.finishFuture().get();

        long secondCpAwaitBeginTime = time.getByName(secondCpAwaitBeginLabel);

        String msg = "waitNextCpTime=" + secondCpAwaitBeginTime + ", maxCheckCancelTime=" + maxCheckCancelTime.get();

        Assert.assertTrue(msg, secondCpAwaitBeginTime < maxCheckCancelTime.get() * 2);

        log.info(msg);
    }

    @Test
    public void testCancelOnDestroyPartitions() throws Exception {
        partitions = 1024;

        String firstCpTimeLable = "firstCpTime";
        String secondCpAwaitBeginLabel = "secondCpAwaitBeginTime";

        Ignite ig = startGrid(0);

        ig.cluster().active(true);

        GridCacheDatabaseSharedManager dbMgr = dbManager(ig);

        GridCacheDatabaseSharedManager.Checkpointer checkpointer = U.field(dbMgr, "checkpointer");

        Function<GridCacheDatabaseSharedManager.Checkpoint, Boolean> prevFunc = checkpointer.checkCancel;

        CountDownLatch awaitDestroyParts = new CountDownLatch(1);
        CountDownLatch awaitContinueDestroyParts = new CountDownLatch(1);

        AtomicLong lastCheckTime = new AtomicLong();
        AtomicLong maxCheckCancelTime = new AtomicLong();

        checkpointer.checkCancel = (chp) -> {
            CheckpointProgress cpProgress = chp.checkpointProgress();

            long last = lastCheckTime.getAndSet(System.currentTimeMillis());

            if (last != 0) {
                long lastCheck = System.currentTimeMillis() - last;

                if (lastCheck > maxCheckCancelTime.get())
                    maxCheckCancelTime.set(lastCheck);
            }

            int destroyQueueSize = cpProgress.destroyQueue().size();

            try {
                U.sleep(5);
            }
            catch (IgniteInterruptedCheckedException e) {
                Thread.currentThread().interrupt();

                throw new IgniteException(e);
            }

            if ((destroyQueueSize < partitions - (partitions / 10)) && awaitDestroyParts.getCount() > 0) {
                awaitDestroyParts.countDown();

                try {
                    log.info("Done to wait destory partitions in current checkpoint, parts:" + destroyQueueSize);

                    awaitContinueDestroyParts.await();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                    throw new IgniteException(e);
                }
            }

            return prevFunc.apply(chp);
        };

        for (int i = 0; i < partitions; i++) {
            int grpId = CU.cacheId(DEFAULT_CACHE_NAME);

            pageMemory(ig, DEFAULT_CACHE_NAME).invalidate(grpId, i);

            dbMgr.schedulePartitionDestroy(grpId, i);
        }

        CheckpointFuture cp1Future = dbMgr.forceCheckpoint("chp=1", cancelCp);

        cp1Future.beginFuture().get();

        time.begin(firstCpTimeLable);

        cp1Future.finishFuture()
            .listen(f -> {
                time.end(firstCpTimeLable);

                log.info("chp=1 time duration (" + time.getByName(firstCpTimeLable) + ")");
            });

        awaitDestroyParts.await();

        Assert.assertTrue(!cp1Future.finishFuture().isDone());

        time.begin(secondCpAwaitBeginLabel);

        CheckpointFuture chpFuture = dbMgr.forceCheckpoint("chp=2, chp=1 canceled", cancelCp);

        awaitContinueDestroyParts.countDown();

        chpFuture.beginFuture().get();

        time.end(secondCpAwaitBeginLabel);

        chpFuture.finishFuture().get();

        long secondCpAwaitBeginTime = time.getByName(secondCpAwaitBeginLabel);

        String msg = "waitNextCpTime=" + secondCpAwaitBeginTime + ", maxCheckCancelTime=" + maxCheckCancelTime.get();

        Assert.assertTrue(msg, secondCpAwaitBeginTime < maxCheckCancelTime.get() * 4);

        log.info(msg);
    }

    @Test
    public void testCancelOnPageWriteAndCrash() {

    }

    @Test
    public void testCancelOnSyncStoresAndCrash() {

    }

    @Test
    public void testCancelOnSyncDestroyPartitionsAndCrash() {

    }

    @Test
    public void testCancelOnPageWriteAndCrashOnNextCheckpoint() {

    }

    @Test
    public void testCancelOnSyncStoresAndCrashOnNextCheckpoint() {

    }

    private long executeTime(Runnable r) {
        long start = U.currentTimeMillis();

        r.run();

        return U.currentTimeMillis() - start;
    }

    private static PageMemoryImpl pageMemory(Ignite ig,String cacheName) {
        return (PageMemoryImpl)((IgniteEx)ig).context().cache().cache(cacheName).context().dataRegion().pageMemory();
    }

    private static GridCacheDatabaseSharedManager dbManager(Ignite ig) {
        return (GridCacheDatabaseSharedManager)((IgniteEx)ig).context().cache().context().database();
    }

    private static class TestFileIOFactory implements FileIOFactory {

        /** Delegate factory. */
        private final FileIOFactory delegate;

        private final List<TestFileIO> files;

        private TestFileIOFactory(FileIOFactory delegate, List<TestFileIO> files) {
            this.delegate = delegate;
            this.files = files;
        }

        @Override public FileIO create(File file, OpenOption... modes) throws IOException {
            if (!file.getName().contains(PART_FILE_PREFIX))
                return delegate.create(file, modes);

            TestFileIO filePartition = new TestFileIO(delegate.create(file, modes));

            files.add(filePartition);

            return filePartition;
        }
    }

    private static class TestFileIO extends FileIODecorator {

        private static final AtomicLong SHARED_PAGE_COUNTER = new AtomicLong();

        /**
         * @param delegate File I/O delegate
         */
        public TestFileIO(FileIO delegate) {
            super(delegate);
        }

        @Override public int write(ByteBuffer srcBuf, long position) throws IOException {
            long cnt;

            if ((cnt = SHARED_PAGE_COUNTER.incrementAndGet()) % 1000 == 0)
                System.out.println(">>> written pages:" + cnt);

            try {
                U.sleep(5);
            }
            catch (IgniteInterruptedCheckedException e) {
                throw new IgniteException(e);
            }

            return super.write(srcBuf, position);
        }

        private static void reset() {
            SHARED_PAGE_COUNTER.set(0);
        }

        private static void await(long bound) throws IgniteInterruptedCheckedException {
            while (SHARED_PAGE_COUNTER.get() < bound)
                U.sleep(50);
        }
    }

    private static class TimeTracker {

        private Map<String, Long> executeTime = new ConcurrentHashMap<>();

        private void begin(String name) {
            executeTime.put(name, System.currentTimeMillis());
        }

        private void end(String name) {
            Long startTime = executeTime.get(name);

            assert startTime != null && startTime > 0;

            executeTime.put(name, (System.currentTimeMillis() - startTime));
        }

        private long getByName(String name) {
            return executeTime.get(name);
        }
    }
}
