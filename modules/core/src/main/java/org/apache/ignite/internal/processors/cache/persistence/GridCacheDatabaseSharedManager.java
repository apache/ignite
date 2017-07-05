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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanRegistrationException;
import javax.management.ObjectName;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.PersistenceMetrics;
import org.apache.ignite.configuration.DataPageEvictionMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.MemoryConfiguration;
import org.apache.ignite.configuration.MemoryPolicyConfiguration;
import org.apache.ignite.configuration.PersistentStoreConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.mem.DirectMemoryProvider;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.pagemem.PageMemory;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.pagemem.snapshot.SnapshotOperation;
import org.apache.ignite.internal.pagemem.store.IgnitePageStoreManager;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.pagemem.wal.StorageException;
import org.apache.ignite.internal.pagemem.wal.WALIterator;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.CacheState;
import org.apache.ignite.internal.pagemem.wal.record.CheckpointRecord;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.MemoryRecoveryRecord;
import org.apache.ignite.internal.pagemem.wal.record.PageSnapshot;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PageDeltaRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionDestroyRecord;
import org.apache.ignite.internal.pagemem.wal.record.delta.PartitionMetaStateRecord;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLocalPartition;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.CheckpointMetricsTracker;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryEx;
import org.apache.ignite.internal.processors.cache.persistence.pagemem.PageMemoryImpl;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWALPointer;
import org.apache.ignite.internal.processors.cache.persistence.wal.crc.PureJavaCrc32;
import org.apache.ignite.internal.processors.port.GridPortRecord;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.GridMultiCollectionWrapper;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.future.CountDownFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.GridInClosure3X;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P3;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.mxbean.PersistenceMetricsMXBean;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_SKIP_CRC;
import static org.apache.ignite.IgniteSystemProperties.IGNITE_PDS_WAL_REBALANCE_THRESHOLD;

/**
 *
 */
@SuppressWarnings({"unchecked", "NonPrivateFieldAccessedInSynchronizedContext"})
public class GridCacheDatabaseSharedManager extends IgniteCacheDatabaseSharedManager {
    /** */
    public static final String IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC = "IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC";

    /** Skip sync. */
    private final boolean skipSync = IgniteSystemProperties.getBoolean(IGNITE_PDS_CHECKPOINT_TEST_SKIP_SYNC);

    /** */
    private boolean skipCrc = IgniteSystemProperties.getBoolean(IGNITE_PDS_SKIP_CRC, false);

    /** */
    private final int walRebalanceThreshold = IgniteSystemProperties.getInteger(
        IGNITE_PDS_WAL_REBALANCE_THRESHOLD, 500_000);

    /** Checkpoint lock hold count. */
    private static final ThreadLocal<Integer> CHECKPOINT_LOCK_HOLD_COUNT = new ThreadLocal<Integer>() {
        @Override protected Integer initialValue() {
            return 0;
        }
    };

    /** Assertion enabled. */
    private static final boolean ASSERTION_ENABLED = GridCacheDatabaseSharedManager.class.desiredAssertionStatus();

    /** Checkpoint file name pattern. */
    private static final Pattern CP_FILE_NAME_PATTERN = Pattern.compile("(\\d+)-(.*)-(START|END)\\.bin");

    /** */
    private static final FileFilter CP_FILE_FILTER = new FileFilter() {
        @Override public boolean accept(File f) {
            return CP_FILE_NAME_PATTERN.matcher(f.getName()).matches();
        }
    };

    /** */
    private static final Comparator<GridDhtLocalPartition> ASC_PART_COMPARATOR = new Comparator<GridDhtLocalPartition>() {
        @Override public int compare(GridDhtLocalPartition a, GridDhtLocalPartition b) {
            return Integer.compare(a.id(), b.id());
        }
    };

    /** */
    private static final Comparator<File> CP_TS_COMPARATOR = new Comparator<File>() {
        /** {@inheritDoc} */
        @Override public int compare(File o1, File o2) {
            Matcher m1 = CP_FILE_NAME_PATTERN.matcher(o1.getName());
            Matcher m2 = CP_FILE_NAME_PATTERN.matcher(o2.getName());

            boolean s1 = m1.matches();
            boolean s2 = m2.matches();

            assert s1 : "Failed to match CP file: " + o1.getAbsolutePath();
            assert s2 : "Failed to match CP file: " + o2.getAbsolutePath();

            long ts1 = Long.parseLong(m1.group(1));
            long ts2 = Long.parseLong(m2.group(1));

            int res = Long.compare(ts1, ts2);

            if (res == 0) {
                CheckpointEntryType type1 = CheckpointEntryType.valueOf(m1.group(3));
                CheckpointEntryType type2 = CheckpointEntryType.valueOf(m2.group(3));

                assert type1 != type2 : "o1=" + o1.getAbsolutePath() + ", o2=" + o2.getAbsolutePath();

                res = type1 == CheckpointEntryType.START ? -1 : 1;
            }

            return res;
        }
    };

    /** */
    private static final String MBEAN_NAME = "PersistenceMetrics";

    /** */
    private static final String MBEAN_GROUP = "Persistent Store";

    /** Checkpoint thread. Needs to be volatile because it is created in exchange worker. */
    private volatile Checkpointer checkpointer;

    /** For testing only. */
    private volatile boolean checkpointsEnabled = true;

    /** For testing only. */
    private volatile GridFutureAdapter<Void> enableChangeApplied;

    /** */
    private ReentrantReadWriteLock checkpointLock = new ReentrantReadWriteLock();

    /** */
    private long checkpointFreq;

    /** */
    private long checkpointPageBufSize;

    /** */
    private FilePageStoreManager storeMgr;

    /** Checkpoint metadata directory ("cp"), contains files with checkpoint start and end */
    private File cpDir;

    /** */
    private volatile boolean printCheckpointStats = true;

    /** Database configuration. */
    private final PersistentStoreConfiguration persistenceCfg;

    /** */
    private final Collection<DbCheckpointListener> lsnrs = new CopyOnWriteArrayList<>();

    /** Checkpoint history. */
    private final CheckpointHistory checkpointHist = new CheckpointHistory();

    /** */
    private boolean stopping;

    /** Checkpoint runner thread pool. */
    private ExecutorService asyncRunner;

    /** Buffer for the checkpoint threads. */
    private ThreadLocal<ByteBuffer> threadBuf;

    /** */
    private final ConcurrentMap<Integer, IgniteInternalFuture> idxRebuildFuts = new ConcurrentHashMap<>();

    /** Lock holder. */
    private FileLockHolder fileLockHolder;

    /** Lock wait time. */
    private final int lockWaitTime;

    /** */
    private Map<Integer, Map<Integer, T2<Long, WALPointer>>> reservedForExchange;

    /** */
    private final ConcurrentMap<T2<Integer, Integer>, T2<Long, WALPointer>> reservedForPreloading = new ConcurrentHashMap<>();

    /** Snapshot manager. */
    private IgniteCacheSnapshotManager snapshotMgr;

    /** */
    private PersistenceMetricsImpl persStoreMetrics;

    /** */
    private ObjectName persistenceMetricsMbeanName;

    /**
     * @param ctx Kernal context.
     */
    public GridCacheDatabaseSharedManager(GridKernalContext ctx) {
        IgniteConfiguration cfg = ctx.config();

        persistenceCfg = cfg.getPersistentStoreConfiguration();

        assert persistenceCfg != null : "PageStore should not be created if persistence is disabled.";

        checkpointFreq = persistenceCfg.getCheckpointingFrequency();

        lockWaitTime = persistenceCfg.getLockWaitTime();

        final int pageSize = cfg.getMemoryConfiguration().getPageSize();

        threadBuf = new ThreadLocal<ByteBuffer>() {
            /** {@inheritDoc} */
            @Override protected ByteBuffer initialValue() {
                ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize);

                tmpWriteBuf.order(ByteOrder.nativeOrder());

                return tmpWriteBuf;
            }
        };

        persStoreMetrics = new PersistenceMetricsImpl(
            persistenceCfg.isMetricsEnabled(),
            persistenceCfg.getRateTimeInterval(),
            persistenceCfg.getSubIntervals()
        );
    }

    /**
     *
     */
    public Checkpointer getCheckpointer() {
        return checkpointer;
    }

    /**
     * For test use only.
     */
    public IgniteInternalFuture<Void> enableCheckpoints(boolean enable) {
        GridFutureAdapter<Void> fut = new GridFutureAdapter<>();

        enableChangeApplied = fut;

        checkpointsEnabled = enable;

        wakeupForCheckpoint("enableCheckpoints()");

        return fut;
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        super.start0();

        snapshotMgr = cctx.snapshot();

        if (!cctx.kernalContext().clientNode()) {
            IgnitePageStoreManager store = cctx.pageStore();

            assert store instanceof FilePageStoreManager : "Invalid page store manager was created: " + store;

            storeMgr = (FilePageStoreManager)store;

            cpDir = Paths.get(storeMgr.workDir().getAbsolutePath(), "cp").toFile();

            if (!U.mkdirs(cpDir))
                throw new IgniteCheckedException("Could not create directory for checkpoint metadata: " + cpDir);

            fileLockHolder = new FileLockHolder(storeMgr.workDir().getPath(), cctx.kernalContext(), log);

            persStoreMetrics.wal(cctx.wal());
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void initDataBase() throws IgniteCheckedException {
        Long cpBufSize = persistenceCfg.getCheckpointingPageBufferSize();

        if (persistenceCfg.getCheckpointingThreads() > 1)
            asyncRunner = new ThreadPoolExecutor(
                persistenceCfg.getCheckpointingThreads(),
                persistenceCfg.getCheckpointingThreads(),
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>()
            );

        // Intentionally use identity comparison to check if configuration default has changed.
        //noinspection NumberEquality
        if (cpBufSize == PersistentStoreConfiguration.DFLT_CHECKPOINTING_PAGE_BUFFER_SIZE) {
            MemoryConfiguration memCfg = cctx.kernalContext().config().getMemoryConfiguration();

            assert memCfg != null;

            long totalSize = memCfg.getSystemCacheMaxSize();

            if (memCfg.getMemoryPolicies() == null)
                totalSize += MemoryConfiguration.DFLT_MEMORY_POLICY_MAX_SIZE;
            else {
                for (MemoryPolicyConfiguration memPlc : memCfg.getMemoryPolicies()) {
                    if (Long.MAX_VALUE - memPlc.getMaxSize() > totalSize)
                        totalSize += memPlc.getMaxSize();
                    else {
                        totalSize = Long.MAX_VALUE;

                        break;
                    }
                }

                assert totalSize > 0;
            }

            // Limit the checkpoint page buffer size by 2GB.
            long dfltSize = 2 * 1024L * 1024L * 1024L;

            long adjusted = Math.min(totalSize / 4, dfltSize);

            if (cpBufSize < adjusted) {
                U.quietAndInfo(log,
                    "Default checkpoint page buffer size is too small, setting to an adjusted value: "
                        + U.readableSize(adjusted, false)
                );

                cpBufSize = adjusted;
            }
        }

        checkpointPageBufSize = cpBufSize;
    }

    /** {@inheritDoc} */
    @Override protected void initPageMemoryDataStructures(MemoryConfiguration dbCfg) throws IgniteCheckedException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void onActivate(GridKernalContext ctx) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Activate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        snapshotMgr = cctx.snapshot();

        if (!cctx.localNode().isClient()) {
            initDataBase();

            registrateMetricsMBean();
        }

        super.onActivate(ctx);
    }

    /** {@inheritDoc} */
    @Override public void onDeActivate(GridKernalContext kctx) {
        if (log.isDebugEnabled())
            log.debug("DeActivate database manager [id=" + cctx.localNodeId() +
                " topVer=" + cctx.discovery().topologyVersionEx() + " ]");

        onKernalStop0(false);

        stop0(false);

        /* Must be here, because after deactivate we can invoke activate and file lock must be already configured */
        stopping = false;

        if (!cctx.localNode().isClient())
            fileLockHolder = new FileLockHolder(storeMgr.workDir().getPath(), cctx.kernalContext(), log);
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void registrateMetricsMBean() throws IgniteCheckedException {
        try {
            persistenceMetricsMbeanName = U.registerMBean(
                cctx.kernalContext().config().getMBeanServer(),
                cctx.kernalContext().igniteInstanceName(),
                MBEAN_GROUP,
                MBEAN_NAME,
                persStoreMetrics,
                PersistenceMetricsMXBean.class);
        }
        catch (JMException e) {
            throw new IgniteCheckedException("Failed to register " + MBEAN_NAME + " MBean.", e);
        }
    }

    /**
     *
     */
    private void unRegistrateMetricsMBean() {
        if (persistenceMetricsMbeanName != null) {
            try {
                cctx.kernalContext().config().getMBeanServer().unregisterMBean(persistenceMetricsMbeanName);
            }
            catch (InstanceNotFoundException ignore) {
                // No-op, nothing to unregister.
            }
            catch (MBeanRegistrationException e) {
                U.error(log, "Failed to unregister " + MBEAN_NAME + " MBean.", e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void readCheckpointAndRestoreMemory(List<DynamicCacheDescriptor> cachesToStart) throws IgniteCheckedException {
        checkpointReadLock();

        try {
            if (!F.isEmpty(cachesToStart)) {
                for (DynamicCacheDescriptor desc : cachesToStart) {
                    if (CU.affinityNode(cctx.localNode(), desc.cacheConfiguration().getNodeFilter()))
                        storeMgr.initializeForCache(desc.groupDescriptor(), new StoredCacheData(desc.cacheConfiguration()));
                }
            }

            CheckpointStatus status = readCheckpointStatus();

            // First, bring memory to the last consistent checkpoint state if needed.
            // This method should return a pointer to the last valid record in the WAL.
            WALPointer restore = restoreMemory(status);

            cctx.wal().resumeLogging(restore);

            cctx.wal().log(new MemoryRecoveryRecord(U.currentTimeMillis()));
        }
        catch (StorageException e) {
            throw new IgniteCheckedException(e);
        }
        finally {
            checkpointReadUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void lock() throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Try to capture file lock [nodeId=" +
                cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

        fileLockHolder.tryLock(lockWaitTime);
    }

    /** {@inheritDoc} */
    @Override public void unLock() {
        if (log.isDebugEnabled())
            log.debug("Release file lock [nodeId=" +
                cctx.localNodeId() + " path=" + fileLockHolder.lockPath() + "]");

        fileLockHolder.release();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        checkpointLock.writeLock().lock();

        try {
            stopping = true;
        }
        finally {
            checkpointLock.writeLock().unlock();
        }

        shutdownCheckpointer(cancel);

        lsnrs.clear();

        super.onKernalStop0(cancel);

        if (!cctx.kernalContext().clientNode()) {
            unLock();

            fileLockHolder.close();
        }

        unRegistrateMetricsMBean();
    }

    /** */
    private long[] calculateFragmentSizes(int concLvl, long cacheSize) {
        if (concLvl < 2)
            concLvl = Runtime.getRuntime().availableProcessors();

        long fragmentSize = cacheSize / concLvl;

        if (fragmentSize < 1024 * 1024)
            fragmentSize = 1024 * 1024;

        long[] sizes = new long[concLvl + 1];

        for (int i = 0; i < concLvl; i++)
            sizes[i] = fragmentSize;

        sizes[concLvl] = checkpointPageBufSize;

        return sizes;
    }

    /** {@inheritDoc} */
    @Override protected PageMemory createPageMemory(
        DirectMemoryProvider memProvider,
        MemoryConfiguration memCfg,
        MemoryPolicyConfiguration plcCfg,
        MemoryMetricsImpl memMetrics
    ) {
        memMetrics.persistenceEnabled(true);

        PageMemoryImpl pageMem = new PageMemoryImpl(
            memProvider,
            calculateFragmentSizes(
                memCfg.getConcurrencyLevel(),
                plcCfg.getMaxSize()
            ),
            cctx,
            memCfg.getPageSize(),
            new GridInClosure3X<FullPageId, ByteBuffer, Integer>() {
                @Override public void applyx(
                    FullPageId fullId,
                    ByteBuffer pageBuf,
                    Integer tag
                ) throws IgniteCheckedException {
                    // First of all, write page to disk.
                    storeMgr.write(fullId.cacheId(), fullId.pageId(), pageBuf, tag);

                    // Only after write we can write page into snapshot.
                    snapshotMgr.flushDirtyPageHandler(fullId, pageBuf, tag);
                }
            },
            new GridInClosure3X<Long, FullPageId, PageMemoryEx>() {
                @Override public void applyx(
                    Long page,
                    FullPageId fullId,
                    PageMemoryEx pageMem
                ) throws IgniteCheckedException {
                    snapshotMgr.onChangeTrackerPage(page, fullId, pageMem);
                }
            },
            this,
            memMetrics
        );

        memMetrics.pageMemory(pageMem);

        return pageMem;
    }

    /** {@inheritDoc} */
    @Override protected void checkPolicyEvictionProperties(MemoryPolicyConfiguration plcCfg, MemoryConfiguration dbCfg)
        throws IgniteCheckedException {
        if (plcCfg.getPageEvictionMode() != DataPageEvictionMode.DISABLED)
            throw new IgniteCheckedException("Page eviction is not compatible with persistence: " + plcCfg.getName());
    }

    /**
     * @param cancel Cancel flag.
     */
    @SuppressWarnings("unused")
    private void shutdownCheckpointer(boolean cancel) {
        Checkpointer cp = checkpointer;

        if (cp != null) {
            if (cancel)
                cp.shutdownNow();
            else
                cp.cancel();

            try {
                U.join(cp);

                checkpointer = null;
            }
            catch (IgniteInterruptedCheckedException ignore) {
                U.warn(log, "Was interrupted while waiting for checkpointer shutdown, " +
                    "will not wait for checkpoint to finish.");

                cp.shutdownNow();

                while (true) {
                    try {
                        U.join(cp);

                        checkpointer = null;

                        cp.scheduledCp.cpFinishFut.onDone(
                            new NodeStoppingException("Checkpointer is stopped during node stop."));

                        break;
                    }
                    catch (IgniteInterruptedCheckedException ignored) {
                        //Ignore
                    }
                }

                Thread.currentThread().interrupt();
            }
        }

        if (asyncRunner != null) {
            asyncRunner.shutdownNow();

            try {
                asyncRunner.awaitTermination(2, TimeUnit.MINUTES);
            }
            catch (InterruptedException ignore) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void beforeExchange(GridDhtPartitionsExchangeFuture fut) throws IgniteCheckedException {
        DiscoveryEvent discoEvt = fut.discoveryEvent();

        boolean joinEvt = discoEvt.type() == EventType.EVT_NODE_JOINED;

        boolean locNode = discoEvt.eventNode().isLocal();

        boolean isSrvNode = !cctx.kernalContext().clientNode();

        boolean clusterInTransitionStateToActive = fut.activateCluster();

        // Before local node join event.
        if (clusterInTransitionStateToActive || (joinEvt && locNode && isSrvNode))
            restoreState();

        if (cctx.kernalContext().query().moduleEnabled()) {
            for (GridCacheContext cacheCtx : (Collection<GridCacheContext>)cctx.cacheContexts()) {
                if (cacheCtx.startTopologyVersion().equals(fut.topologyVersion()) &&
                    !cctx.pageStore().hasIndexStore(cacheCtx.groupId()) && cacheCtx.affinityNode()) {
                    final int cacheId = cacheCtx.cacheId();

                    final IgniteInternalFuture<?> rebuildFut = cctx.kernalContext().query()
                        .rebuildIndexesFromHash(Collections.singletonList(cacheCtx.cacheId()));

                    idxRebuildFuts.put(cacheId, rebuildFut);

                    rebuildFut.listen(new CI1<IgniteInternalFuture>() {
                        @Override public void apply(IgniteInternalFuture igniteInternalFut) {
                            idxRebuildFuts.remove(cacheId, rebuildFut);
                        }
                    });
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteInternalFuture indexRebuildFuture(int cacheId) {
        return idxRebuildFuts.get(cacheId);
    }

    /** {@inheritDoc} */
    @Override public boolean persistenceEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void onCacheGroupsStopped(
        Collection<IgniteBiTuple<CacheGroupContext, Boolean>> stoppedGrps
    ) {
        Map<PageMemoryEx, Collection<Integer>> destroyed = new HashMap<>();

        for (IgniteBiTuple<CacheGroupContext, Boolean> tup : stoppedGrps) {
            CacheGroupContext gctx = tup.get1();

            snapshotMgr.onCacheGroupStop(gctx);

            PageMemoryEx pageMem = (PageMemoryEx)gctx.memoryPolicy().pageMemory();

            Collection<Integer> grpIds = destroyed.get(pageMem);

            if (grpIds == null) {
                grpIds = new HashSet<>();

                destroyed.put(pageMem, grpIds);
            }

            grpIds.add(tup.get1().groupId());

            pageMem.onCacheGroupDestroyed(tup.get1().groupId());
        }

        Collection<IgniteInternalFuture<Void>> clearFuts = new ArrayList<>(destroyed.size());

        for (Map.Entry<PageMemoryEx, Collection<Integer>> entry : destroyed.entrySet()) {
            final Collection<Integer> grpIds = entry.getValue();

            clearFuts.add(entry.getKey().clearAsync(new P3<Integer, Long, Integer>() {
                @Override public boolean apply(Integer grpId, Long pageId, Integer tag) {
                    return grpIds.contains(grpId);
                }
            }, false));
        }
        for (IgniteInternalFuture<Void> clearFut : clearFuts) {
            try {
                clearFut.get();
            }
            catch (IgniteCheckedException e) {
                log.error("Failed to clear page memory", e);
            }
        }

        if (cctx.pageStore() != null) {
            for (IgniteBiTuple<CacheGroupContext, Boolean> tup : stoppedGrps) {
                CacheGroupContext grp = tup.get1();

                if (grp.affinityNode()) {
                    try {
                        cctx.pageStore().shutdownForCacheGroup(grp, tup.get2());
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to gracefully clean page store resources for destroyed cache " +
                            "[cache=" + grp.cacheOrGroupName() + "]", e);
                    }
                }
            }
        }
    }

    /**
     * Gets the checkpoint read lock. While this lock is held, checkpoint thread will not acquiSnapshotWorkerre memory state.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    @Override public void checkpointReadLock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        for (; ; ) {
            checkpointLock.readLock().lock();

            if (stopping) {
                checkpointLock.readLock().unlock();

                throw new RuntimeException("Failed to perform cache update: node is stopping.");
            }

            if (safeToUpdatePageMemories() || checkpointLock.getReadHoldCount() > 1)
                break;
            else {
                checkpointLock.readLock().unlock();

                try {
                    checkpointer.wakeupForCheckpoint(0, "too many dirty pages").cpBeginFut.get();
                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException("Failed to wait for checkpoint begin.", e);
                }
            }
        }

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() + 1);
    }

    /** {@inheritDoc} */
    @Override public boolean checkpointLockIsHeldByThread() {
        return !ASSERTION_ENABLED ||
            checkpointLock.isWriteLockedByCurrentThread() ||
            CHECKPOINT_LOCK_HOLD_COUNT.get() > 0;
    }

    /**
     * @return {@code true} if all PageMemory instances are safe to update.
     */
    private boolean safeToUpdatePageMemories() {
        Collection<MemoryPolicy> memPlcs = context().database().memoryPolicies();

        if (memPlcs == null)
            return true;

        for (MemoryPolicy memPlc : memPlcs) {
            PageMemoryEx pageMemEx = (PageMemoryEx) memPlc.pageMemory();

            if (!pageMemEx.safeToUpdate())
                return false;
        }

        return true;
    }

    /**
     * Releases the checkpoint read lock.
     */
    @Override public void checkpointReadUnlock() {
        if (checkpointLock.writeLock().isHeldByCurrentThread())
            return;

        checkpointLock.readLock().unlock();

        if (checkpointer != null) {
            Collection<MemoryPolicy> memPlcs = context().database().memoryPolicies();

            if (memPlcs != null) {
                for (MemoryPolicy memPlc : memPlcs) {
                    PageMemoryEx mem = (PageMemoryEx)memPlc.pageMemory();

                    if (mem != null && !mem.safeToUpdate()) {
                        checkpointer.wakeupForCheckpoint(0, "too many dirty pages");

                        break;
                    }
                }
            }
        }

        if (ASSERTION_ENABLED)
            CHECKPOINT_LOCK_HOLD_COUNT.set(CHECKPOINT_LOCK_HOLD_COUNT.get() - 1);
    }

    /**
     * @throws IgniteCheckedException If failed to restore database status from WAL.
     */
    private void restoreState() throws IgniteCheckedException {
        try {
            CheckpointStatus status = readCheckpointStatus();

            checkpointReadLock();

            try {
                applyLastUpdates(status);
            }
            finally {
                checkpointReadUnlock();
            }

            snapshotMgr.restoreState();

            checkpointer = new Checkpointer(cctx.igniteInstanceName(), "db-checkpoint-thread", log);

            new IgniteThread(cctx.igniteInstanceName(), "db-checkpoint-thread", checkpointer).start();
        }
        catch (StorageException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public synchronized Map<Integer, Map<Integer, Long>> reserveHistoryForExchange() {
        assert reservedForExchange == null : reservedForExchange;

        reservedForExchange = new HashMap<>();

        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions()) {
                if (part.state() != GridDhtPartitionState.OWNING || part.dataStore().fullSize() <= walRebalanceThreshold)
                    continue;

                CheckpointEntry cpEntry = searchCheckpointEntry(grp.groupId(), part.id(), null);

                try {
                    if (cpEntry != null && cctx.wal().reserve(cpEntry.cpMark)) {
                        Map<Integer, T2<Long, WALPointer>> cacheMap = reservedForExchange.get(grp.groupId());

                        if (cacheMap == null) {
                            cacheMap = new HashMap<>();

                            reservedForExchange.put(grp.groupId(), cacheMap);
                        }

                        cacheMap.put(part.id(), new T2<>(cpEntry.partitionCounter(grp.groupId(), part.id()), cpEntry.cpMark));
                    }
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Error while trying to reserve history", ex);
                }
            }
        }

        Map<Integer, Map<Integer, Long>> resMap = new HashMap<>();

        for (Map.Entry<Integer, Map<Integer, T2<Long, WALPointer>>> e : reservedForExchange.entrySet()) {
            Map<Integer, Long> cacheMap = new HashMap<>();

            for (Map.Entry<Integer, T2<Long, WALPointer>> e0 : e.getValue().entrySet())
                cacheMap.put(e0.getKey(), e0.getValue().get1());

            resMap.put(e.getKey(), cacheMap);
        }

        return resMap;
    }

    /** {@inheritDoc} */
    @Override public synchronized void releaseHistoryForExchange() {
        if (reservedForExchange == null)
            return;

        for (Map.Entry<Integer, Map<Integer, T2<Long, WALPointer>>> e : reservedForExchange.entrySet()) {
            for (Map.Entry<Integer, T2<Long, WALPointer>> e0 : e.getValue().entrySet()) {
                try {
                    cctx.wal().release(e0.getValue().get2());
                }
                catch (IgniteCheckedException ex) {
                    U.error(log, "Could not release history lock", ex);
                }
            }
        }

        reservedForExchange = null;
    }

    /** {@inheritDoc} */
    @Override public boolean reserveHistoryForPreloading(int grpId, int partId, long cntr) {
        CheckpointEntry cpEntry = searchCheckpointEntry(grpId, partId, cntr);

        if (cpEntry == null)
            return false;

        WALPointer ptr = cpEntry.cpMark;

        if (ptr == null)
            return false;

        boolean reserved;

        try {
            reserved = cctx.wal().reserve(ptr);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Error while trying to reserve history", e);

            reserved = false;
        }

        if (reserved)
            reservedForPreloading.put(new T2<>(grpId, partId), new T2<>(cntr, ptr));

        return reserved;
    }

    /** {@inheritDoc} */
    @Override public void releaseHistoryForPreloading() {
        for (Map.Entry<T2<Integer, Integer>, T2<Long, WALPointer>> e : reservedForPreloading.entrySet()) {
            try {
                cctx.wal().release(e.getValue().get2());
            }
            catch (IgniteCheckedException ex) {
                U.error(log, "Could not release WAL reservation", ex);

                throw new IgniteException(ex);
            }
        }

        reservedForPreloading.clear();
    }

    /**
     * For debugging only. TODO: remove.
     *
     */
    public Map<T2<Integer, Integer>, T2<Long, WALPointer>> reservedForPreloading() {
        return reservedForPreloading;
    }

    /**
     *
     */
    @Nullable @Override public IgniteInternalFuture wakeupForCheckpoint(String reason) {
        Checkpointer cp = checkpointer;

        if (cp != null)
            return cp.wakeupForCheckpoint(0, reason).cpBeginFut;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void waitForCheckpoint(String reason) throws IgniteCheckedException {
        Checkpointer cp = checkpointer;

        if (cp == null)
            return;

        CheckpointProgressSnapshot progSnapshot = cp.wakeupForCheckpoint(0, reason);

        IgniteInternalFuture fut1 = progSnapshot.cpFinishFut;

        fut1.get();

        if (!progSnapshot.started)
            return;

        IgniteInternalFuture fut2 = cp.wakeupForCheckpoint(0, reason).cpFinishFut;

        assert fut1 != fut2;

        fut2.get();
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable public WALPointer searchPartitionCounter(int grpId, int part, @Nullable Long partCntrSince) {
        CheckpointEntry entry = searchCheckpointEntry(grpId, part, partCntrSince);

        if (entry == null)
            return null;

        return entry.cpMark;
    }

    /**
     * Tries to search for a WAL pointer for the given partition counter start.
     *
     * @param grpId Cache group ID.
     * @param part Partition ID.
     * @param partCntrSince Partition counter or {@code null} to search for minimal counter.
     * @return Checkpoint entry or {@code null} if failed to search.
     */
    @Nullable private CheckpointEntry searchCheckpointEntry(int grpId, int part, @Nullable Long partCntrSince) {
        boolean hasGap = false;
        CheckpointEntry first = null;

        for (Long cpTs : checkpointHist.checkpoints()) {
            try {
                CheckpointEntry entry = checkpointHist.entry(cpTs);

                Long foundCntr = entry.partitionCounter(grpId, part);

                if (foundCntr != null) {
                    if (partCntrSince == null) {
                        if (hasGap) {
                            first = entry;

                            hasGap = false;
                        }

                        if (first == null)
                            first = entry;
                    }
                    else if (foundCntr <= partCntrSince) {
                        first = entry;

                        hasGap = false;
                    }
                    else
                        return hasGap ? null : first;
                }
                else
                    hasGap = true;
            }
            catch (IgniteCheckedException ignore) {
                // Treat exception the same way as a gap.
                hasGap = true;
            }
        }

        return hasGap ? null : first;
    }

    /**
     * @return Checkpoint history. For tests only.
     */
    public CheckpointHistory checkpointHistory() {
        return checkpointHist;
    }

    /**
     * @return Checkpoint directory.
     */
    public File checkpointDirectory() {
        return cpDir;
    }

    /**
     * @param lsnr Listener.
     */
    public void addCheckpointListener(DbCheckpointListener lsnr) {
        lsnrs.add(lsnr);
    }

    /**
     * @param lsnr Listener.
     */
    public void removeCheckpointListener(DbCheckpointListener lsnr) {
        lsnrs.remove(lsnr);
    }

    /**
     * @return Read checkpoint status.
     * @throws IgniteCheckedException If failed to read checkpoint status page.
     */
    @SuppressWarnings("TooBroadScope")
    private CheckpointStatus readCheckpointStatus() throws IgniteCheckedException {
        long lastStartTs = 0;
        long lastEndTs = 0;

        UUID startId = CheckpointStatus.NULL_UUID;
        UUID endId = CheckpointStatus.NULL_UUID;

        File startFile = null;
        File endFile = null;

        WALPointer startPtr = CheckpointStatus.NULL_PTR;
        WALPointer endPtr = CheckpointStatus.NULL_PTR;

        File dir = cpDir;

        if (!dir.exists()) {
            // TODO: remove excessive logging after GG-12116 fix.
            File[] files = dir.listFiles();

            if (files != null && files.length > 0) {
                log.warning("Read checkpoint status: cpDir.exists() is false, cpDir.listFiles() is: " +
                    Arrays.toString(files));
            }

            if (Files.exists(dir.toPath()))
                log.warning("Read checkpoint status: cpDir.exists() is false, Files.exists(cpDir) is true.");

            log.info("Read checkpoint status: checkpoint directory is not found.");

            return new CheckpointStatus(0, startId, startPtr, endId, endPtr);
        }

        File[] files = dir.listFiles();

        for (File file : files) {
            Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

            if (matcher.matches()) {
                long ts = Long.parseLong(matcher.group(1));
                UUID id = UUID.fromString(matcher.group(2));
                CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

                if (type == CheckpointEntryType.START && ts > lastStartTs) {
                    lastStartTs = ts;
                    startId = id;
                    startFile = file;
                }
                else if (type == CheckpointEntryType.END && ts > lastEndTs) {
                    lastEndTs = ts;
                    endId = id;
                    endFile = file;
                }
            }
        }

        ByteBuffer buf = ByteBuffer.allocate(20);
        buf.order(ByteOrder.nativeOrder());

        if (startFile != null)
            startPtr = readPointer(startFile, buf);

        if (endFile != null)
            endPtr = readPointer(endFile, buf);

        // TODO: remove excessive logging after GG-12116 fix.
        log.info("Read checkpoint status: start marker = " + startFile + ", end marker = " + endFile);

        return new CheckpointStatus(lastStartTs, startId, startPtr, endId, endPtr);
    }

    /**
     * Loads WAL pointer from CP file
     * @param cpMarkerFile Checkpoint mark file.
     * @return WAL pointer.
     * @throws IgniteCheckedException If failed to read mark file.
     */
    private WALPointer readPointer(File cpMarkerFile, ByteBuffer buf) throws IgniteCheckedException {
        buf.position(0);

        try (FileChannel ch = FileChannel.open(cpMarkerFile.toPath(), StandardOpenOption.READ)) {
            ch.read(buf);

            buf.flip();

            return new FileWALPointer(buf.getLong(), buf.getInt(), buf.getInt());
        }
        catch (IOException e) {
            throw new IgniteCheckedException("Failed to read checkpoint pointer from marker file: " +
                cpMarkerFile.getAbsolutePath(), e);
        }
    }

    /**
     * @param status Checkpoint status.
     */
    private WALPointer restoreMemory(CheckpointStatus status) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Checking memory state [lastValidPos=" + status.endPtr + ", lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        boolean apply = status.needRestoreMemory();

        if (apply) {
            U.quietAndWarn(log, "Ignite node crashed in the middle of checkpoint. Will restore memory state and " +
                "enforce checkpoint on node start.");

            cctx.pageStore().beginRecover();
        }

        long start = U.currentTimeMillis();
        int applied = 0;
        WALPointer lastRead = null;

        try (WALIterator it = cctx.wal().replay(status.endPtr)) {
            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                WALRecord rec = tup.get2();

                lastRead = tup.get1();

                switch (rec.type()) {
                    case CHECKPOINT_RECORD:
                        CheckpointRecord cpRec = (CheckpointRecord)rec;

                        // We roll memory up until we find a checkpoint start record registered in the status.
                        if (F.eq(cpRec.checkpointId(), status.cpStartId)) {
                            log.info("Found last checkpoint marker [cpId=" + cpRec.checkpointId() +
                                ", pos=" + tup.get1() + ']');

                            apply = false;
                        }
                        else if (!F.eq(cpRec.checkpointId(), status.cpEndId))
                            U.warn(log, "Found unexpected checkpoint marker, skipping [cpId=" + cpRec.checkpointId() +
                                ", expCpId=" + status.cpStartId + ", pos=" + tup.get1() + ']');

                        break;

                    case PAGE_RECORD:
                        if (apply) {
                            PageSnapshot pageRec = (PageSnapshot)rec;

                            // Here we do not require tag check because we may be applying memory changes after
                            // several repetitive restarts and the same pages may have changed several times.
                            int cacheId = pageRec.fullPageId().cacheId();
                            long pageId = pageRec.fullPageId().pageId();

                            PageMemoryEx pageMem = getPageMemoryForCacheGroup(cacheId);

                            long page = pageMem.acquirePage(cacheId, pageId, true);

                            try {
                                long pageAddr = pageMem.writeLock(cacheId, pageId, page);

                                try {
                                    PageUtils.putBytes(pageAddr, 0, pageRec.pageData());
                                }
                                finally {
                                    pageMem.writeUnlock(cacheId, pageId, page, null, true, true);
                                }
                            }
                            finally {
                                pageMem.releasePage(cacheId, pageId, page);
                            }

                            applied++;
                        }

                        break;

                    case PARTITION_DESTROY:
                        if (apply) {
                            PartitionDestroyRecord destroyRec = (PartitionDestroyRecord)rec;

                            final int cId = destroyRec.cacheId();
                            final int pId = destroyRec.partitionId();

                            PageMemoryEx pageMem = getPageMemoryForCacheGroup(cId);

                            pageMem.clearAsync(new P3<Integer, Long, Integer>() {
                                @Override public boolean apply(Integer cacheId, Long pageId, Integer tag) {
                                    return cacheId == cId && PageIdUtils.partId(pageId) == pId;
                                }
                            }, true).get();
                        }

                        break;

                    default:
                        if (apply && rec instanceof PageDeltaRecord) {
                            PageDeltaRecord r = (PageDeltaRecord)rec;

                            int cacheId = r.cacheId();
                            long pageId = r.pageId();

                            PageMemoryEx pageMem = getPageMemoryForCacheGroup(cacheId);

                            // Here we do not require tag check because we may be applying memory changes after
                            // several repetitive restarts and the same pages may have changed several times.
                            long page = pageMem.acquirePage(cacheId, pageId, true);

                            try {
                                long pageAddr = pageMem.writeLock(cacheId, pageId, page);

                                try {
                                    r.applyDelta(pageMem, pageAddr);
                                }
                                finally {
                                    pageMem.writeUnlock(cacheId, pageId, page, null, true, true);
                                }
                            }
                            finally {
                                pageMem.releasePage(cacheId, pageId, page);
                            }

                            applied++;
                        }
                }
            }
        }

        if (status.needRestoreMemory()) {
            if (apply)
                throw new IgniteCheckedException("Failed to restore memory state (checkpoint marker is present " +
                    "on disk, but checkpoint record is missed in WAL) " +
                    "[cpStatus=" + status + ", lastRead=" + lastRead + "]");

            log.info("Finished applying memory changes [changesApplied=" + applied +
                ", time=" + (U.currentTimeMillis() - start) + "ms]");

            if (applied > 0)
                finalizeCheckpointOnRecovery(status.cpStartTs, status.cpStartId, status.startPtr);
        }

        checkpointHist.loadHistory(cpDir);

        return lastRead == null ? null : lastRead.next();
    }

    /**
     * Obtains PageMemory reference from cache descriptor instead of cache context.
     *
     * @param grpId Cache group id.
     * @return PageMemoryEx instance.
     * @throws IgniteCheckedException if no MemoryPolicy is configured for a name obtained from cache descriptor.
     */
    private PageMemoryEx getPageMemoryForCacheGroup(int grpId) throws IgniteCheckedException {
        // TODO IGNITE-5075: cache descriptor can be removed.
        GridCacheSharedContext sharedCtx = context();

        String memPlcName = sharedCtx
            .cache()
            .cacheGroupDescriptors().get(grpId)
            .config()
            .getMemoryPolicyName();

        return (PageMemoryEx)sharedCtx.database().memoryPolicy(memPlcName).pageMemory();
    }

    /**
     * @param status Last registered checkpoint status.
     * @throws IgniteCheckedException If failed to apply updates.
     * @throws StorageException If IO exception occurred while reading write-ahead log.
     */
    private void applyLastUpdates(CheckpointStatus status) throws IgniteCheckedException {
        if (log.isInfoEnabled())
            log.info("Applying lost cache updates since last checkpoint record [lastMarked="
                + status.startPtr + ", lastCheckpointId=" + status.cpStartId + ']');

        cctx.kernalContext().query().skipFieldLookup(true);

        long start = U.currentTimeMillis();
        int applied = 0;

        try (WALIterator it = cctx.wal().replay(status.startPtr)) {
            Map<T2<Integer, Integer>, T2<Integer, Long>> partStates = new HashMap<>();

            while (it.hasNextX()) {
                IgniteBiTuple<WALPointer, WALRecord> next = it.nextX();

                WALRecord rec = next.get2();

                switch (rec.type()) {
                    case DATA_RECORD:
                        DataRecord dataRec = (DataRecord)rec;

                        for (DataEntry dataEntry : dataRec.writeEntries()) {
                            int cacheId = dataEntry.cacheId();

                            GridCacheContext cacheCtx = cctx.cacheContext(cacheId);

                            applyUpdate(cacheCtx, dataEntry);

                            applied++;
                        }

                        break;

                    case PART_META_UPDATE_STATE:
                        PartitionMetaStateRecord metaStateRecord = (PartitionMetaStateRecord)rec;

                        partStates.put(new T2<>(metaStateRecord.cacheId(), metaStateRecord.partitionId()),
                            new T2<>((int)metaStateRecord.state(), metaStateRecord.updateCounter()));

                        break;

                    default:
                        // Skip other records.
                }
            }

            restorePartitionState(partStates);
        }
        finally {
            cctx.kernalContext().query().skipFieldLookup(false);
        }

        if (log.isInfoEnabled())
            log.info("Finished applying WAL changes [updatesApplied=" + applied +
                ", time=" + (U.currentTimeMillis() - start) + "ms]");
    }

    /**
     * @param partStates Partition states.
     * @throws IgniteCheckedException If failed to restore.
     */
    private void restorePartitionState(
        Map<T2<Integer, Integer>, T2<Integer, Long>> partStates
    ) throws IgniteCheckedException {
        for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
            if (grp.isLocal() || !grp.affinityNode()) {
                // Local cache has no partitions and its states.
                continue;
            }

            int grpId = grp.groupId();

            PageMemoryEx pageMem = (PageMemoryEx)grp.memoryPolicy().pageMemory();

            for (int i = 0; i < grp.affinity().partitions(); i++) {
                if (storeMgr.exists(grpId, i)) {
                    storeMgr.ensure(grpId, i);

                    if (storeMgr.pages(grpId, i) <= 1)
                        continue;

                    long partMetaId = pageMem.partitionMetaPageId(grpId, i);
                    long partMetaPage = pageMem.acquirePage(grpId, partMetaId);
                    try {
                        long pageAddr = pageMem.writeLock(grpId, partMetaId, partMetaPage);

                        boolean changed = false;

                        try {
                            PagePartitionMetaIO io = PagePartitionMetaIO.VERSIONS.forPage(pageAddr);

                            T2<Integer, Long> fromWal = partStates.get(new T2<>(grpId, i));

                            GridDhtLocalPartition part = grp.topology()
                                .localPartition(i, AffinityTopologyVersion.NONE, true);

                            assert part != null;

                            if (fromWal != null) {
                                int stateId = fromWal.get1();

                                io.setPartitionState(pageAddr, (byte)stateId);

                                changed = updateState(part, stateId);

                                if (stateId == GridDhtPartitionState.OWNING.ordinal()) {
                                    grp.offheap().onPartitionInitialCounterUpdated(i, fromWal.get2());

                                    if (part.initialUpdateCounter() < fromWal.get2()) {
                                        part.initialUpdateCounter(fromWal.get2());

                                        changed = true;
                                    }
                                }
                            }
                            else
                                changed = updateState(part, (int)io.getPartitionState(pageAddr));
                        }
                        finally {
                            pageMem.writeUnlock(grpId, partMetaId, partMetaPage, null, changed);
                        }
                    }
                    finally {
                        pageMem.releasePage(grpId, partMetaId, partMetaPage);
                    }
                }
            }
        }
    }

    /**
     * @param part Partition to restore state for.
     * @param stateId State enum ordinal.
     * @return Updated flag.
     */
    private boolean updateState(GridDhtLocalPartition part, int stateId) {
        if (stateId != -1) {
            GridDhtPartitionState state = GridDhtPartitionState.fromOrdinal(stateId);

            assert state != null;

            part.restoreState(state == GridDhtPartitionState.EVICTED ? GridDhtPartitionState.RENTING : state);

            return true;
        }

        return false;
    }

    /**
     * @param cacheCtx Cache context to apply an update.
     * @param dataEntry Data entry to apply.
     */
    private void applyUpdate(GridCacheContext cacheCtx, DataEntry dataEntry) throws IgniteCheckedException {
        GridDhtLocalPartition locPart = cacheCtx.topology()
            .localPartition(dataEntry.partitionId(), AffinityTopologyVersion.NONE, true);

        switch (dataEntry.op()) {
            case CREATE:
            case UPDATE:
                cacheCtx.offheap().update(
                    cacheCtx,
                    dataEntry.key(),
                    dataEntry.value(),
                    dataEntry.writeVersion(),
                    0L,
                    locPart,
                    null);

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().onPartitionInitialCounterUpdated(dataEntry.partitionId(), dataEntry.partitionCounter());

                break;

            case DELETE:
                cacheCtx.offheap().remove(cacheCtx, dataEntry.key(), dataEntry.partitionId(), locPart);

                if (dataEntry.partitionCounter() != 0)
                    cacheCtx.offheap().onPartitionInitialCounterUpdated(dataEntry.partitionId(), dataEntry.partitionCounter());

                break;

            default:
                throw new IgniteCheckedException("Invalid operation for WAL entry update: " + dataEntry.op());
        }
    }

    /**
     * @throws IgniteCheckedException If failed.
     */
    private void finalizeCheckpointOnRecovery(long cpTs, UUID cpId, WALPointer walPtr) throws IgniteCheckedException {
        assert cpTs != 0;

        ByteBuffer tmpWriteBuf = ByteBuffer.allocateDirect(pageSize());

        long start = System.currentTimeMillis();

        Collection<MemoryPolicy> memPolicies = context().database().memoryPolicies();

        List<IgniteBiTuple<PageMemory, Collection<FullPageId>>> cpEntities = new ArrayList<>(memPolicies.size());

        for (MemoryPolicy memPlc : memPolicies) {
            PageMemoryEx pageMem = (PageMemoryEx) memPlc.pageMemory();
            cpEntities.add(new IgniteBiTuple<PageMemory, Collection<FullPageId>>(pageMem,
                (pageMem).beginCheckpoint()));
        }

        tmpWriteBuf.order(ByteOrder.nativeOrder());

        // Identity stores set.
        Collection<PageStore> updStores = new HashSet<>();

        int cpPagesCnt = 0;

        for (IgniteBiTuple<PageMemory, Collection<FullPageId>> e : cpEntities) {
            PageMemoryEx pageMem = (PageMemoryEx) e.get1();

            Collection<FullPageId> cpPages = e.get2();

            cpPagesCnt += cpPages.size();

            for (FullPageId fullId : cpPages) {
                tmpWriteBuf.rewind();

                Integer tag = pageMem.getForCheckpoint(fullId, tmpWriteBuf, null);

                if (tag != null) {
                    tmpWriteBuf.rewind();

                    PageStore store = storeMgr.writeInternal(fullId.cacheId(), fullId.pageId(), tmpWriteBuf, tag);

                    tmpWriteBuf.rewind();

                    updStores.add(store);
                }
            }
        }

        long written = U.currentTimeMillis();

        for (PageStore updStore : updStores)
            updStore.sync();

        long fsync = U.currentTimeMillis();

        for (IgniteBiTuple<PageMemory, Collection<FullPageId>> e : cpEntities)
            ((PageMemoryEx)e.get1()).finishCheckpoint();

        writeCheckpointEntry(
            tmpWriteBuf,
            cpTs,
            cpId,
            walPtr,
            null,
            CheckpointEntryType.END);

        cctx.pageStore().finishRecover();

        if (log.isInfoEnabled())
            log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                    "pagesWrite=%dms, fsync=%dms, total=%dms]",
                cpId,
                cpPagesCnt,
                walPtr,
                written - start,
                fsync - written,
                fsync - start));
    }

    /**
     * @param cpId Checkpoint ID.
     * @param ptr Wal pointer of current checkpoint.
     */
    private CheckpointEntry writeCheckpointEntry(
        ByteBuffer tmpWriteBuf,
        long cpTs,
        UUID cpId,
        WALPointer ptr,
        CheckpointRecord rec,
        CheckpointEntryType type
    ) throws IgniteCheckedException {
        assert ptr instanceof FileWALPointer;

        FileWALPointer filePtr = (FileWALPointer)ptr;

        String fileName = checkpointFileName(cpTs, cpId, type);

        try (FileChannel ch = FileChannel.open(Paths.get(cpDir.getAbsolutePath(), fileName),
            StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND)) {

            tmpWriteBuf.rewind();

            tmpWriteBuf.putLong(filePtr.index());

            tmpWriteBuf.putInt(filePtr.fileOffset());

            tmpWriteBuf.putInt(filePtr.length());

            tmpWriteBuf.flip();

            ch.write(tmpWriteBuf);

            tmpWriteBuf.clear();

            if (!skipSync)
                ch.force(true);

            return type == CheckpointEntryType.START ?
                new CheckpointEntry(cpTs, ptr, cpId, rec.cacheGroupStates()) : null;
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
    }

    /**
     * @param cpTs Checkpoint timestamp.
     * @param cpId Checkpoint ID.
     * @param type Checkpoint type.
     * @return Checkpoint file name.
     */
    private static String checkpointFileName(long cpTs, UUID cpId, CheckpointEntryType type) {
        return cpTs + "-" + cpId + "-" + type + ".bin";
    }

    /**
     *
     */
    @SuppressWarnings("NakedNotify")
    public class Checkpointer extends GridWorker {
        /** Temporary write buffer. */
        private final ByteBuffer tmpWriteBuf;

        /** Next scheduled checkpoint progress. */
        private volatile CheckpointProgress scheduledCp;

        /** Current checkpoint. This field is updated only by checkpoint thread. */
        private volatile CheckpointProgress curCpProgress;

        /** Shutdown now. */
        private volatile boolean shutdownNow;

        /**
         * @param gridName Grid name.
         * @param name Thread name.
         * @param log Logger.
         */
        protected Checkpointer(@Nullable String gridName, String name, IgniteLogger log) {
            super(gridName, name, log);

            scheduledCp = new CheckpointProgress(U.currentTimeMillis() + checkpointFreq);

            tmpWriteBuf = ByteBuffer.allocateDirect(pageSize());

            tmpWriteBuf.order(ByteOrder.nativeOrder());
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            while (!isCancelled()) {
                waitCheckpointEvent();

                GridFutureAdapter<Void> enableChangeApplied = GridCacheDatabaseSharedManager.this.enableChangeApplied;

                if (enableChangeApplied != null) {
                    enableChangeApplied.onDone();

                    GridCacheDatabaseSharedManager.this.enableChangeApplied = null;
                }

                if (checkpointsEnabled)
                    doCheckpoint();
                else {
                    synchronized (this) {
                        scheduledCp.nextCpTs = U.currentTimeMillis() + checkpointFreq;
                    }
                }
            }

            // Final run after the cancellation.
            if (checkpointsEnabled && !shutdownNow)
                doCheckpoint();

            scheduledCp.cpFinishFut.onDone(new NodeStoppingException("Node is stopping."));
        }

        /**
         *
         */
        private CheckpointProgressSnapshot wakeupForCheckpoint(long delayFromNow, String reason) {
            CheckpointProgress sched = scheduledCp;

            long next = U.currentTimeMillis() + delayFromNow;

            if (sched.nextCpTs <= next)
                return new CheckpointProgressSnapshot(sched);

            CheckpointProgressSnapshot ret;

            synchronized (this) {
                sched = scheduledCp;

                if (sched.nextCpTs > next) {
                    sched.reason = reason;

                    sched.nextCpTs = next;
                }

                ret = new CheckpointProgressSnapshot(sched);

                notifyAll();
            }

            return ret;
        }

        /**
         * @param snapshotOperation Snapshot operation.
         */
        public IgniteInternalFuture wakeupForSnapshotCreation(SnapshotOperation snapshotOperation) {
            GridFutureAdapter<Object> ret;

            synchronized (this) {
                scheduledCp.nextCpTs = U.currentTimeMillis();

                scheduledCp.reason = "snapshot";

                scheduledCp.nextSnapshot = true;

                scheduledCp.snapshotOperation = snapshotOperation;

                ret = scheduledCp.cpBeginFut;

                notifyAll();
            }

            return ret;
        }

        /**
         *
         */
        private void doCheckpoint() {
            try {
                CheckpointMetricsTracker tracker = new CheckpointMetricsTracker();

                Checkpoint chp = markCheckpointBegin(tracker);

                boolean interrupted = true;

                try {
                    if (chp.hasDelta()) {
                        // Identity stores set.
                        GridConcurrentHashSet<PageStore> updStores = new GridConcurrentHashSet<>();

                        CountDownFuture doneWriteFut = new CountDownFuture(
                            asyncRunner == null ? 1 : chp.cpPages.collectionsSize());

                        tracker.onPagesWriteStart();

                        if (asyncRunner != null) {
                            for (int i = 0; i < chp.cpPages.collectionsSize(); i++) {
                                Runnable write = new WriteCheckpointPages(
                                    tracker,
                                    chp.cpPages.innerCollection(i),
                                    updStores,
                                    doneWriteFut
                                );

                                try {
                                    asyncRunner.execute(write);
                                }
                                catch (RejectedExecutionException ignore) {
                                    // Run the task synchronously.
                                    write.run();
                                }
                            }
                        }
                        else {
                            // Single-threaded checkpoint.
                            Runnable write = new WriteCheckpointPages(tracker, chp.cpPages, updStores, doneWriteFut);

                            write.run();
                        }

                        // Wait and check for errors.
                        doneWriteFut.get();

                        // Must re-check shutdown flag here because threads may have skipped some pages.
                        // If so, we should not put finish checkpoint mark.
                        if (shutdownNow) {
                            chp.progress.cpFinishFut.onDone(new NodeStoppingException("Node is stopping."));

                            return;
                        }

                        tracker.onFsyncStart();

                        if (!skipSync) {
                            for (PageStore updStore : updStores) {
                                if (shutdownNow) {
                                    chp.progress.cpFinishFut.onDone(new NodeStoppingException("Node is stopping."));

                                    return;
                                }

                                updStore.sync();
                            }
                        }
                    }
                    else {
                        tracker.onPagesWriteStart();
                        tracker.onFsyncStart();
                    }

                    snapshotMgr.afterCheckpointPageWritten();

                    // Must mark successful checkpoint only if there are no exceptions or interrupts.
                    interrupted = false;
                }
                finally {
                    if (!interrupted)
                        markCheckpointEnd(chp);
                }

                tracker.onEnd();

                if (chp.hasDelta()) {
                    if (printCheckpointStats) {
                        if (log.isInfoEnabled())
                            log.info(String.format("Checkpoint finished [cpId=%s, pages=%d, markPos=%s, " +
                                    "walSegmentsCleared=%d, markDuration=%dms, pagesWrite=%dms, fsync=%dms, " +
                                    "total=%dms]",
                                chp.cpEntry.checkpointId(),
                                chp.pagesSize,
                                chp.cpEntry.checkpointMark(),
                                chp.walFilesDeleted,
                                tracker.markDuration(),
                                tracker.pagesWriteDuration(),
                                tracker.fsyncDuration(),
                                tracker.totalDuration()));
                    }

                    persStoreMetrics.onCheckpoint(
                        tracker.lockWaitDuration(),
                        tracker.markDuration(),
                        tracker.pagesWriteDuration(),
                        tracker.fsyncDuration(),
                        tracker.totalDuration(),
                        chp.pagesSize,
                        tracker.dataPagesWritten(),
                        tracker.cowPagesWritten());
                }
                else {
                    persStoreMetrics.onCheckpoint(
                        tracker.lockWaitDuration(),
                        tracker.markDuration(),
                        tracker.pagesWriteDuration(),
                        tracker.fsyncDuration(),
                        tracker.totalDuration(),
                        chp.pagesSize,
                        tracker.dataPagesWritten(),
                        tracker.cowPagesWritten());
                }
            }
            catch (IgniteCheckedException e) {
                // TODO-ignite-db how to handle exception?
                U.error(log, "Failed to create checkpoint.", e);
            }
        }

        /**
         *
         */
        @SuppressWarnings("WaitNotInLoop")
        private void waitCheckpointEvent() {
            boolean cancel = false;

            try {
                long now = U.currentTimeMillis();

                synchronized (this) {
                    long remaining;

                    while ((remaining = scheduledCp.nextCpTs - now) > 0 && !isCancelled()) {
                        wait(remaining);

                        now = U.currentTimeMillis();
                    }
                }
            }
            catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();

                cancel = true;
            }

            if (cancel)
                isCancelled = true;
        }

        /**
         *
         */
        @SuppressWarnings("TooBroadScope")
        private Checkpoint markCheckpointBegin(CheckpointMetricsTracker tracker) throws IgniteCheckedException {
            CheckpointRecord cpRec = new CheckpointRecord(null);

            WALPointer cpPtr = null;

            GridMultiCollectionWrapper<FullPageId> cpPages;

            final CheckpointProgress curr;

            tracker.onLockWaitStart();

            checkpointLock.writeLock().lock();

            try {
                tracker.onMarkStart();

                synchronized (this) {
                    curr = scheduledCp;

                    curr.started = true;

                    if (curr.reason == null)
                        curr.reason = "timeout";

                    // It is important that we assign a new progress object before checkpoint mark in page memory.
                    scheduledCp = new CheckpointProgress(U.currentTimeMillis() + checkpointFreq);

                    curCpProgress = curr;
                }

                final NavigableMap<T2<Integer, Integer>, T2<Integer, Integer>> map =
                    new TreeMap<>(FullPageIdIterableComparator.INSTANCE);

                DbCheckpointListener.Context ctx0 = new DbCheckpointListener.Context() {
                    @Override public boolean nextSnapshot() {
                        return curr.nextSnapshot;
                    }

                    @Override public Map<T2<Integer, Integer>, T2<Integer, Integer>> partitionStatMap() {
                        return map;
                    }
                };

                // Listeners must be invoked before we write checkpoint record to WAL.
                for (DbCheckpointListener lsnr : lsnrs)
                    lsnr.onCheckpointBegin(ctx0);

                for (CacheGroupContext grp : cctx.cache().cacheGroups()) {
                    if (grp.isLocal())
                        continue;

                    List<GridDhtLocalPartition> locParts = new ArrayList<>();

                    for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                        locParts.add(part);

                    Collections.sort(locParts, ASC_PART_COMPARATOR);

                    CacheState state = new CacheState(locParts.size());

                    for (GridDhtLocalPartition part : grp.topology().currentLocalPartitions())
                        state.addPartitionState(part.id(), part.dataStore().fullSize(), part.lastAppliedUpdate());

                    cpRec.addCacheGroupState(grp.groupId(), state);
                }

                if (curr.nextSnapshot)
                    snapshotMgr.onMarkCheckPointBegin(curr.snapshotOperation, map);

                IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> tup = beginAllCheckpoints();

                // Todo it maybe more optimally
                Collection<FullPageId> cpPagesList = new ArrayList<>(tup.get2());

                for (GridMultiCollectionWrapper<FullPageId> col : tup.get1()) {
                    for (int i = 0; i < col.collectionsSize(); i++)
                        cpPagesList.addAll(col.innerCollection(i));
                }

                cpPages = new GridMultiCollectionWrapper<>(cpPagesList);

                if (!F.isEmpty(cpPages)) {
                    // No page updates for this checkpoint are allowed from now on.
                    cpPtr = cctx.wal().log(cpRec);

                    if (cpPtr == null)
                        cpPtr = CheckpointStatus.NULL_PTR;
                }
            }
            finally {
                checkpointLock.writeLock().unlock();

                tracker.onLockRelease();
            }

            curr.cpBeginFut.onDone();

            if (!F.isEmpty(cpPages)) {
                assert cpPtr != null;

                // Sync log outside the checkpoint write lock.
                cctx.wal().fsync(cpPtr);

                long cpTs = System.currentTimeMillis();

                CheckpointEntry cpEntry = writeCheckpointEntry(
                    tmpWriteBuf,
                    cpTs,
                    cpRec.checkpointId(),
                    cpPtr,
                    cpRec,
                    CheckpointEntryType.START);

                checkpointHist.addCheckpointEntry(cpEntry);

                if (printCheckpointStats)
                    if (log.isInfoEnabled())
                        log.info(String.format("Checkpoint started [checkpointId=%s, startPtr=%s, checkpointLockWait=%dms, " +
                                "checkpointLockHoldTime=%dms, pages=%d, reason='%s']",
                            cpRec.checkpointId(),
                            cpPtr,
                            tracker.lockWaitDuration(),
                            tracker.lockHoldDuration(),
                            cpPages.size(),
                            curr.reason)
                        );

                return new Checkpoint(cpEntry, cpPages, curr);
            }
            else {
                if (printCheckpointStats) {
                    if (log.isInfoEnabled())
                        LT.info(log, String.format("Skipping checkpoint (no pages were modified) [" +
                            "checkpointLockWait=%dms, checkpointLockHoldTime=%dms, reason='%s']",
                            tracker.lockWaitDuration(),
                            tracker.lockHoldDuration(),
                            curr.reason));
                }

                GridMultiCollectionWrapper<FullPageId> wrapper = new GridMultiCollectionWrapper<>(new Collection[0]);

                return new Checkpoint(null, wrapper, curr);
            }
        }

        /**
         * @return tuple with collections of FullPageIds obtained from each PageMemory and overall number of dirty pages.
         */
        private IgniteBiTuple<Collection<GridMultiCollectionWrapper<FullPageId>>, Integer> beginAllCheckpoints() {
            Collection<GridMultiCollectionWrapper<FullPageId>> res = new ArrayList(memoryPolicies().size());

            int pagesNum = 0;

            for (MemoryPolicy memPlc : memoryPolicies()) {
                GridMultiCollectionWrapper<FullPageId> nextCpPagesCol = ((PageMemoryEx) memPlc.pageMemory()).beginCheckpoint();

                pagesNum += nextCpPagesCol.size();

                res.add(nextCpPagesCol);
            }

            return new IgniteBiTuple<>(res, pagesNum);
        }

        /**
         * @param chp Checkpoint snapshot.
         */
        private void markCheckpointEnd(Checkpoint chp) throws IgniteCheckedException {
            synchronized (this) {
                for (MemoryPolicy memPlc : memoryPolicies())
                    ((PageMemoryEx)memPlc.pageMemory()).finishCheckpoint();

                if (chp.hasDelta())
                    writeCheckpointEntry(
                        tmpWriteBuf,
                        chp.cpEntry.checkpointTimestamp(),
                        chp.cpEntry.checkpointId(),
                        chp.cpEntry.checkpointMark(),
                        null,
                        CheckpointEntryType.END);
            }

            checkpointHist.onCheckpointFinished(chp);

            if (chp.progress != null)
                chp.progress.cpFinishFut.onDone();
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            if (log.isDebugEnabled())
                log.debug("Cancelling grid runnable: " + this);

            // Do not interrupt runner thread.
            isCancelled = true;

            synchronized (this) {
                notifyAll();
            }
        }

        /**
         *
         */
        public void shutdownNow() {
            shutdownNow = true;

            if (!isCancelled)
                cancel();
        }
    }

    /**
     *
     */
    private class WriteCheckpointPages implements Runnable {
        /** */
        private CheckpointMetricsTracker tracker;

        /** */
        private Collection<FullPageId> writePageIds;

        /** */
        private GridConcurrentHashSet<PageStore> updStores;

        /** */
        private CountDownFuture doneFut;

        /**
         * @param writePageIds Write page IDs.
         */
        private WriteCheckpointPages(
            CheckpointMetricsTracker tracker,
            Collection<FullPageId> writePageIds,
            GridConcurrentHashSet<PageStore> updStores,
            CountDownFuture doneFut
        ) {
            this.tracker = tracker;
            this.writePageIds = writePageIds;
            this.updStores = updStores;
            this.doneFut = doneFut;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            ByteBuffer tmpWriteBuf = threadBuf.get();

            long writeAddr = GridUnsafe.bufferAddress(tmpWriteBuf);

            snapshotMgr.beforeCheckpointPageWritten();

            try {
                for (FullPageId fullId : writePageIds) {
                    if (checkpointer.shutdownNow)
                        break;

                    tmpWriteBuf.rewind();

                    snapshotMgr.beforePageWrite(fullId);

                    int grpId = fullId.cacheId();

                    CacheGroupContext grp = context().cache().cacheGroup(grpId);

                    if (grp == null)
                        continue;

                    PageMemoryEx pageMem = (PageMemoryEx)grp.memoryPolicy().pageMemory();

                    Integer tag = pageMem.getForCheckpoint(
                        fullId, tmpWriteBuf, persStoreMetrics.metricsEnabled() ? tracker : null);

                    if (tag != null) {
                        tmpWriteBuf.rewind();

                        if (persStoreMetrics.metricsEnabled()) {
                            int pageType = PageIO.getType(tmpWriteBuf);

                            if (PageIO.isDataPageType(pageType))
                                tracker.onDataPageWritten();
                        }

                        if (!skipCrc) {
                            PageIO.setCrc(writeAddr, PureJavaCrc32.calcCrc32(tmpWriteBuf, pageSize()));

                            tmpWriteBuf.rewind();
                        }

                        snapshotMgr.onPageWrite(fullId, tmpWriteBuf);

                        tmpWriteBuf.rewind();

                        PageIO.setCrc(writeAddr, 0);

                        PageStore store = storeMgr.writeInternal(grpId, fullId.pageId(), tmpWriteBuf, tag);

                        updStores.add(store);
                    }
                }

                doneFut.onDone((Void)null);
            }
            catch (Throwable e) {
                doneFut.onDone(e);
            }
        }
    }

    /**
     *
     */
    private enum CheckpointEntryType {
        /** */
        START,

        /** */
        END
    }

    /**
     *
     */
    private static class Checkpoint {
        /** Checkpoint entry. */
        private final CheckpointEntry cpEntry;

        /** Checkpoint pages. */
        private final GridMultiCollectionWrapper<FullPageId> cpPages;

        /** */
        private final CheckpointProgress progress;

        /** Number of deleted WAL files. */
        private int walFilesDeleted;

        /** */
        private final int pagesSize;

        /**
         * @param cpEntry Checkpoint entry.
         * @param cpPages Pages to write to the page store.
         * @param progress Checkpoint progress status.
         */
        private Checkpoint(
            CheckpointEntry cpEntry,
            @NotNull GridMultiCollectionWrapper<FullPageId> cpPages,
            CheckpointProgress progress
        ) {
            assert cpEntry == null || cpEntry.initGuard != 0;

            this.cpEntry = cpEntry;
            this.cpPages = cpPages;
            this.progress = progress;

            pagesSize = cpPages.size();
        }

        /**
         * @return {@code true} if this checkpoint contains at least one dirty page.
         */
        private boolean hasDelta() {
            return pagesSize != 0;
        }
    }

    /**
     *
     */
    private static class CheckpointStatus {
        /** Null checkpoint UUID. */
        private static final UUID NULL_UUID = new UUID(0L, 0L);

        /** Null WAL pointer. */
        private static final WALPointer NULL_PTR = new FileWALPointer(0, 0, 0);

        /** */
        private long cpStartTs;

        /** */
        private UUID cpStartId;

        /** */
        @GridToStringInclude
        private WALPointer startPtr;

        /** */
        private UUID cpEndId;

        /** */
        @GridToStringInclude
        private WALPointer endPtr;

        /**
         * @param cpStartId Checkpoint start ID.
         * @param startPtr Checkpoint start pointer.
         * @param cpEndId Checkpoint end ID.
         * @param endPtr Checkpoint end pointer.
         */
        private CheckpointStatus(long cpStartTs, UUID cpStartId, WALPointer startPtr, UUID cpEndId, WALPointer endPtr) {
            this.cpStartTs = cpStartTs;
            this.cpStartId = cpStartId;
            this.startPtr = startPtr;
            this.cpEndId = cpEndId;
            this.endPtr = endPtr;
        }

        /**
         * @return {@code True} if need to apply page log to restore tree structure.
         */
        public boolean needRestoreMemory() {
            return !F.eq(cpStartId, cpEndId) && !F.eq(NULL_UUID, cpStartId);
        }

        /** {@inheritDoc} */
        public String toString() {
            return S.toString(CheckpointStatus.class, this);
        }
    }

    /**
     *
     */
    private static class CheckpointProgress {
        /** */
        private volatile long nextCpTs;

        /** */
        private GridFutureAdapter cpBeginFut = new GridFutureAdapter<>();

        /** */
        private GridFutureAdapter cpFinishFut = new GridFutureAdapter<>();

        /** */
        private volatile boolean nextSnapshot;

        /** */
        private volatile boolean started;

        /** */
        private volatile SnapshotOperation snapshotOperation;

        /** Wakeup reason. */
        private String reason;

        /**
         * @param nextCpTs Next checkpoint timestamp.
         */
        private CheckpointProgress(long nextCpTs) {
            this.nextCpTs = nextCpTs;
        }
    }

    /**
     *
     */
    private static class CheckpointProgressSnapshot {
        /** */
        private final boolean started;

        /** */
        private final GridFutureAdapter<Object> cpBeginFut;

        /** */
        private final GridFutureAdapter<Object> cpFinishFut;

        /** */
        CheckpointProgressSnapshot(CheckpointProgress cpProgress) {
            started = cpProgress.started;
            cpBeginFut = cpProgress.cpBeginFut;
            cpFinishFut = cpProgress.cpFinishFut;
        }
    }

    /**
     * Checkpoint history. Holds chronological ordered map with {@link GridCacheDatabaseSharedManager.CheckpointEntry CheckpointEntries}.
     * Data is loaded from corresponding checkpoint directory.
     * This directory holds files for checkpoint start and end.
     */
    @SuppressWarnings("PublicInnerClass")
    public class CheckpointHistory {
        /**
         * Maps checkpoint's timestamp (from CP file name) to CP entry.
         * Using TS provides historical order of CP entries in map ( first is oldest )
         */
        private final NavigableMap<Long, CheckpointEntry> histMap = new ConcurrentSkipListMap<>();

        /**
         * Load history form checkpoint directory.
         *
         * @param dir Checkpoint state dir.
         */
        private void loadHistory(File dir) throws IgniteCheckedException {
            if (!dir.exists())
                return;

            File[] files = dir.listFiles(CP_FILE_FILTER);

            if (!F.isEmpty(files)) {
                Arrays.sort(files, CP_TS_COMPARATOR);

                ByteBuffer buf = ByteBuffer.allocate(16);
                buf.order(ByteOrder.nativeOrder());

                for (File file : files) {
                    Matcher matcher = CP_FILE_NAME_PATTERN.matcher(file.getName());

                    if (matcher.matches()) {
                        CheckpointEntryType type = CheckpointEntryType.valueOf(matcher.group(3));

                        if (type == CheckpointEntryType.START) {
                            long cpTs = Long.parseLong(matcher.group(1));
                            WALPointer ptr = readPointer(file, buf);

                            if (ptr == null)
                                continue;

                            // Create lazy checkpoint entry.
                            CheckpointEntry entry = new CheckpointEntry(cpTs, ptr);

                            histMap.put(cpTs, entry);
                        }
                    }
                }
            }
        }

        /**
         * @param cpTs Checkpoint timestamp.
         * @return Initialized entry.
         * @throws IgniteCheckedException If failed to initialize entry.
         */
        private CheckpointEntry entry(Long cpTs) throws IgniteCheckedException {
            CheckpointEntry entry = histMap.get(cpTs);

            if (entry == null)
                throw new IgniteCheckedException("Checkpoint entry was removed: " + cpTs);

            entry.initIfNeeded(cctx);

            return entry;
        }

        /**
         * @return Collection of checkpoint timestamps.
         */
        public Collection<Long> checkpoints() {
            return histMap.keySet();
        }

        /**
         * Adds checkpoint entry after the corresponding WAL record has been written to WAL. The checkpoint itself
         * is not finished yet.
         *
         * @param entry Entry to ad.
         */
        private void addCheckpointEntry(CheckpointEntry entry) {
            histMap.put(entry.checkpointTimestamp(), entry);
        }

        /**
         * Clears checkpoint history.
         */
        private void onCheckpointFinished(Checkpoint chp) {
            int deleted = 0;

            while (histMap.size() > persistenceCfg.getWalHistorySize()) {
                Map.Entry<Long, CheckpointEntry> entry = histMap.firstEntry();

                CheckpointEntry cpEntry = entry.getValue();

                if (cctx.wal().reserved(cpEntry.checkpointMark())) {
                    U.warn(log, "Could not clear historyMap due to WAL reservation on cpEntry " + cpEntry.cpId +
                        ", history map size is " + histMap.size());

                    break;
                }

                File startFile = new File(cpDir.getAbsolutePath(), cpEntry.startFile());
                File endFile = new File(cpDir.getAbsolutePath(), cpEntry.endFile());

                boolean rmvdStart = !startFile.exists() || startFile.delete();
                boolean rmvdEnd = !endFile.exists() || endFile.delete();

                boolean fail = !rmvdStart || !rmvdEnd;

                if (fail) {
                    U.warn(log, "Failed to remove stale checkpoint files [startFile=" + startFile.getAbsolutePath() +
                        ", endFile=" + endFile.getAbsolutePath() + ']');

                    if (histMap.size() > 2 * persistenceCfg.getWalHistorySize()) {
                        U.error(log, "Too many stale checkpoint entries in the map, will truncate WAL archive anyway.");

                        fail = false;
                    }
                }

                if (!fail) {
                    deleted += cctx.wal().truncate(cpEntry.checkpointMark());

                    histMap.remove(entry.getKey());
                }
                else
                    break;
            }

            chp.walFilesDeleted = deleted;
        }

        /**
         *
         * @param cacheId Cache ID.
         * @param partId Partition ID.
         * @return Reserved counter or null if couldn't reserve.
         */
        @Nullable private Long reserve(int cacheId, int partId) {
            for (CheckpointEntry entry : histMap.values()) {
                try {
                    entry.initIfNeeded(cctx);

                    if (entry.cacheGrpStates == null)
                        continue;

                    CacheState grpState = entry.cacheGrpStates.get(cacheId);

                    if (grpState == null)
                        continue;

                    long partCntr = grpState.counterByPartition(partId);

                    if (partCntr >= 0) {
                        if (cctx.wal().reserve(entry.checkpointMark()))
                            return partCntr;
                    }
                }
                catch (Exception e) {
                    U.error(log, "Error while trying to reserve history", e);
                }
            }

            return null;
        }
    }

    /**
     *
     */
    private static class CheckpointEntry {
        /** */
        private static final AtomicIntegerFieldUpdater<CheckpointEntry> initGuardUpdater =
            AtomicIntegerFieldUpdater.newUpdater(CheckpointEntry.class, "initGuard");

        /** Checkpoint timestamp. */
        private long cpTs;

        /** Checkpoint end mark. */
        private WALPointer cpMark;

        /** Initialization latch. */
        private CountDownLatch initLatch;

        /** */
        @SuppressWarnings("unused")
        private volatile int initGuard;

        /** Checkpoint ID. Initialized lazily. */
        private UUID cpId;

        /** Cache states. Initialized lazily. */
        private Map<Integer, CacheState> cacheGrpStates;

        /** Initialization exception. */
        private IgniteCheckedException initEx;

        /**
         * Lazy entry constructor.
         *
         * @param cpTs Checkpoint timestamp.
         * @param cpMark Checkpoint end mark (WAL pointer).
         */
        private CheckpointEntry(long cpTs, WALPointer cpMark) {
            assert cpMark != null;

            this.cpTs = cpTs;
            this.cpMark = cpMark;

            initLatch = new CountDownLatch(1);
        }

        /**
         * Creates complete entry.
         *
         * @param cpTs Checkpoint timestamp.
         * @param cpMark Checkpoint mark pointer.
         * @param cpId Checkpoint ID.
         * @param cacheGrpStates Cache groups states.
         */
        private CheckpointEntry(long cpTs, WALPointer cpMark, UUID cpId, Map<Integer, CacheState> cacheGrpStates) {
            this.cpTs = cpTs;
            this.cpMark = cpMark;
            this.cpId = cpId;
            this.cacheGrpStates = cacheGrpStates;

            initGuard = 1;
            initLatch = new CountDownLatch(0);
        }

        /**
         * @return Checkpoint timestamp.
         */
        private long checkpointTimestamp() {
            return cpTs;
        }

        /**
         * @return Checkpoint ID.
         */
        private UUID checkpointId() {
            return cpId;
        }

        /**
         * @return Checkpoint mark.
         */
        private WALPointer checkpointMark() {
            return cpMark;
        }

        /**
         * @return Start file name.
         */
        private String startFile() {
            return checkpointFileName(cpTs, cpId, CheckpointEntryType.START);
        }

        /**
         * @return End file name.
         */
        private String endFile() {
            return checkpointFileName(cpTs, cpId, CheckpointEntryType.END);
        }

        /**
         * @param grpId Cache group ID.
         * @param part Partition ID.
         * @return Partition counter or {@code null} if not found.
         */
        private Long partitionCounter(int grpId, int part) {
            assert initGuard != 0;

            if (initEx != null || cacheGrpStates == null)
                return null;

            CacheState state = cacheGrpStates.get(grpId);

            if (state != null) {
                long cntr = state.counterByPartition(part);

                return cntr < 0 ? null : cntr;
            }

            return null;
        }

        /**
         * @throws IgniteCheckedException If failed to read WAL entry.
         */
        private void initIfNeeded(GridCacheSharedContext cctx) throws IgniteCheckedException {
            if (initGuardUpdater.compareAndSet(this, 0, 1)) {
                try (WALIterator it = cctx.wal().replay(cpMark)) {
                    if (it.hasNextX()) {
                        IgniteBiTuple<WALPointer, WALRecord> tup = it.nextX();

                        CheckpointRecord rec = (CheckpointRecord)tup.get2();

                        cpId = rec.checkpointId();
                        cacheGrpStates = rec.cacheGroupStates();
                    }
                    else
                        initEx = new IgniteCheckedException("Failed to find checkpoint record at " +
                            "the given WAL pointer: " + cpMark);
                }
                catch (IgniteCheckedException e) {
                    initEx = e;
                }
                finally {
                    initLatch.countDown();
                }
            }
            else {
                U.await(initLatch);

                if (initEx != null)
                    throw initEx;
            }
        }
    }

    /**
     *
     */
    private static class FileLockHolder {
        /** Lock file name. */
        private static final String lockFileName = "lock";

        /** File. */
        private File file;

        /** Channel. */
        private RandomAccessFile lockFile;

        /** Lock. */
        private FileLock lock;

        /** Id. */
        private GridKernalContext ctx;

        /** Logger. */
        private IgniteLogger log;

        /**
         * @param path Path.
         */
        private FileLockHolder(String path, GridKernalContext ctx, IgniteLogger log) {
            try {
                file = Paths.get(path, lockFileName).toFile();

                lockFile = new RandomAccessFile(file, "rw");

                this.ctx = ctx;
                this.log = log;
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * @param lockWaitTime During which time thread will try capture file lock.
         * @throws IgniteCheckedException If failed to capture file lock.
         */
        public void tryLock(int lockWaitTime) throws IgniteCheckedException {
            assert lockFile != null;

            FileChannel ch = lockFile.getChannel();

            SB sb = new SB();

            //write node id
            sb.a("[").a(ctx.localNodeId().toString()).a("]");

            //write ip addresses
            sb.a(ctx.discovery().localNode().addresses());

            //write ports
            sb.a("[");
            Iterator<GridPortRecord> it = ctx.ports().records().iterator();

            while (it.hasNext()) {
                GridPortRecord rec = it.next();

                sb.a(rec.protocol()).a(":").a(rec.port());

                if (it.hasNext())
                    sb.a(", ");
            }

            sb.a("]");

            String failMsg;

            try {
                String content = null;

                // Try to get lock, if not available wait 1 sec and re-try.
                for (int i = 0; i < lockWaitTime; i += 1000) {
                    try {
                        lock = ch.tryLock(0, 1, false);
                        if (lock != null && lock.isValid()) {
                            writeContent(sb.toString());

                            return;
                        }
                    }
                    catch (OverlappingFileLockException ignore) {
                        if (content == null)
                            content = readContent();

                        log.warning("Failed to acquire file lock (local nodeId:" + ctx.localNodeId()
                            + ", already locked by " + content + "), will try again in 1s: "
                            + file.getAbsolutePath());
                    }

                    U.sleep(1000);
                }

                if (content == null)
                    content = readContent();

                failMsg = "Failed to acquire file lock during " + (lockWaitTime / 1000) +
                    " sec, (locked by " + content + "): " + file.getAbsolutePath();
            }
            catch (Exception e) {
                throw new IgniteCheckedException(e);
            }

            if (failMsg != null)
                throw new IgniteCheckedException(failMsg);
        }

        /**
         * Write node id (who captured lock) into lock file.
         *
         * @param content Node id.
         * @throws IOException if some fail while write node it.
         */
        private void writeContent(String content) throws IOException {
            FileChannel ch = lockFile.getChannel();

            byte[] bytes = content.getBytes();

            ByteBuffer buf = ByteBuffer.allocate(bytes.length);
            buf.put(bytes);

            buf.flip();

            ch.write(buf, 1);

            ch.force(false);
        }

        /**
         *
         */
        private String readContent() throws IOException {
            FileChannel ch = lockFile.getChannel();

            ByteBuffer buf = ByteBuffer.allocate((int)(ch.size() - 1));

            ch.read(buf, 1);

            String content = new String(buf.array());

            buf.clear();

            return content;
        }

        /**
         *
         */
        private void release() {
            U.releaseQuiet(lock);
        }

        /**
         *
         */
        private void close() {
            U.closeQuiet(lockFile);
        }

        /**
         * @return Absolute path to lock file.
         */
        private String lockPath() {
            return file.getAbsolutePath();
        }
    }

    /** {@inheritDoc} */
    @Override public PersistenceMetrics persistentStoreMetrics() {
        return new PersistenceMetricsSnapshot(persStoreMetrics);
    }

    /**
     *
     */
    public PersistenceMetricsImpl persistentStoreMetricsImpl() {
        return persStoreMetrics;
    }
}