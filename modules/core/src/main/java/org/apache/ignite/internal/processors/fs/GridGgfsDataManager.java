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

package org.apache.ignite.internal.processors.fs;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.ignitefs.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.processors.cache.transactions.*;
import org.apache.ignite.internal.processors.dataload.*;
import org.apache.ignite.internal.processors.task.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.internal.util.worker.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.thread.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import javax.cache.processor.*;
import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.*;
import static org.apache.ignite.transactions.IgniteTxConcurrency.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

/**
 * Cache based file's data container.
 */
public class GridGgfsDataManager extends GridGgfsManager {
    /** GGFS. */
    private GridGgfsEx ggfs;

    /** Data cache projection. */
    private GridCacheProjectionEx<GridGgfsBlockKey, byte[]> dataCachePrj;

    /** Data cache. */
    private GridCache<Object, Object> dataCache;

    /** */
    private IgniteInternalFuture<?> dataCacheStartFut;

    /** Local GGFS metrics. */
    private GridGgfsLocalMetrics metrics;

    /** Group block size. */
    private long grpBlockSize;

    /** Group size. */
    private int grpSize;

    /** Byte buffer writer. */
    private ByteBufferBlocksWriter byteBufWriter = new ByteBufferBlocksWriter();

    /** Data input writer. */
    private DataInputBlocksWriter dataInputWriter = new DataInputBlocksWriter();

    /** Pending writes future. */
    private ConcurrentMap<IgniteUuid, WriteCompletionFuture> pendingWrites = new ConcurrentHashMap8<>();

    /** Affinity key generator. */
    private AtomicLong affKeyGen = new AtomicLong();

    /** GGFS executor service. */
    private ExecutorService ggfsSvc;

    /** Request ID counter for write messages. */
    private AtomicLong reqIdCtr = new AtomicLong();

    /** GGFS communication topic. */
    private Object topic;

    /** Async file delete worker. */
    private AsyncDeleteWorker delWorker;

    /** Trash purge timeout. */
    private long trashPurgeTimeout;

    /** On-going remote reads futures. */
    private final ConcurrentHashMap8<GridGgfsBlockKey, IgniteInternalFuture<byte[]>> rmtReadFuts =
        new ConcurrentHashMap8<>();

    /** Executor service for puts in dual mode */
    private volatile ExecutorService putExecSvc;

    /** Executor service for puts in dual mode shutdown flag. */
    private volatile boolean putExecSvcShutdown;

    /** Maximum amount of data in pending puts. */
    private volatile long maxPendingPuts;

    /** Current amount of data in pending puts. */
    private long curPendingPuts;

    /** Lock for pending puts. */
    private final Lock pendingPutsLock = new ReentrantLock();

    /** Condition for pending puts. */
    private final Condition pendingPutsCond = pendingPutsLock.newCondition();

    /**
     *
     */
    void awaitInit() {
        if (!dataCacheStartFut.isDone()) {
            try {
                dataCacheStartFut.get();
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        ggfs = ggfsCtx.ggfs();

        dataCachePrj = ggfsCtx.kernalContext().cache().internalCache(ggfsCtx.configuration().getDataCacheName());
        dataCache = ggfsCtx.kernalContext().cache().internalCache(ggfsCtx.configuration().getDataCacheName());

        dataCacheStartFut = ggfsCtx.kernalContext().cache().internalCache(ggfsCtx.configuration().getDataCacheName())
            .preloader().startFuture();

        if (dataCache.configuration().getAtomicityMode() != TRANSACTIONAL)
            throw new IgniteCheckedException("Data cache should be transactional: " +
                ggfsCtx.configuration().getDataCacheName());

        metrics = ggfsCtx.ggfs().localMetrics();

        assert dataCachePrj != null;

        CacheAffinityKeyMapper mapper = ggfsCtx.kernalContext().cache()
            .internalCache(ggfsCtx.configuration().getDataCacheName()).configuration().getAffinityMapper();

        grpSize = mapper instanceof IgniteFsGroupDataBlocksKeyMapper ?
            ((IgniteFsGroupDataBlocksKeyMapper)mapper).groupSize() : 1;

        grpBlockSize = ggfsCtx.configuration().getBlockSize() * grpSize;

        String ggfsName = ggfsCtx.configuration().getName();

        topic = F.isEmpty(ggfsName) ? TOPIC_GGFS : TOPIC_GGFS.topic(ggfsName);

        ggfsCtx.kernalContext().io().addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (msg instanceof GridGgfsBlocksMessage)
                    processBlocksMessage(nodeId, (GridGgfsBlocksMessage)msg);
                else if (msg instanceof GridGgfsAckMessage)
                    processAckMessage(nodeId, (GridGgfsAckMessage)msg);
            }
        });

        ggfsCtx.kernalContext().event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                if (ggfsCtx.ggfsNode(discoEvt.eventNode())) {
                    for (WriteCompletionFuture future : pendingWrites.values()) {
                        future.onError(discoEvt.eventNode().id(),
                            new ClusterTopologyCheckedException("Node left grid before write completed: " + evt.node().id()));
                    }
                }
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ggfsSvc = ggfsCtx.kernalContext().getGgfsExecutorService();

        trashPurgeTimeout = ggfsCtx.configuration().getTrashPurgeTimeout();

        putExecSvc = ggfsCtx.configuration().getDualModePutExecutorService();

        if (putExecSvc != null)
            putExecSvcShutdown = ggfsCtx.configuration().getDualModePutExecutorServiceShutdown();
        else {
            int coresCnt = Runtime.getRuntime().availableProcessors();

            // Note that we do not pre-start threads here as GGFS pool may not be needed.
            putExecSvc = new IgniteThreadPoolExecutor(coresCnt, coresCnt, 0, new LinkedBlockingDeque<Runnable>());

            putExecSvcShutdown = true;
        }

        maxPendingPuts = ggfsCtx.configuration().getDualModeMaxPendingPutsSize();

        delWorker = new AsyncDeleteWorker(ggfsCtx.kernalContext().gridName(),
            "ggfs-" + ggfsName + "-delete-worker", log);
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        new Thread(delWorker).start();
    }

    /** {@inheritDoc} */
    @Override protected void onKernalStop0(boolean cancel) {
        if (cancel)
            delWorker.cancel();
        else
            delWorker.stop();

        try {
            // Always wait thread exit.
            U.join(delWorker);
        }
        catch (IgniteInterruptedCheckedException e) {
            log.warning("Got interrupter while waiting for delete worker to stop (will continue stopping).", e);
        }

        if (putExecSvcShutdown)
            U.shutdownNow(getClass(), putExecSvc, log);
    }

    /**
     * @return Number of bytes used to store files.
     */
    public long spaceSize() {
        return dataCachePrj.ggfsDataSpaceUsed();
    }

    /**
     * @return Maximum number of bytes for GGFS data cache.
     */
    public long maxSpaceSize() {
        return dataCachePrj.ggfsDataSpaceMax();
    }

    /**
     * Generates next affinity key for local node based on current topology. If previous affinity key maps
     * on local node, return previous affinity key to prevent unnecessary file map growth.
     *
     * @param prevAffKey Affinity key of previous block.
     * @return Affinity key.
     */
    public IgniteUuid nextAffinityKey(@Nullable IgniteUuid prevAffKey) {
        // Do not generate affinity key for non-affinity nodes.
        if (!isAffinityNode(dataCache.configuration()))
            return null;

        UUID nodeId = ggfsCtx.kernalContext().localNodeId();

        if (prevAffKey != null && dataCache.affinity().mapKeyToNode(prevAffKey).isLocal())
            return prevAffKey;

        while (true) {
            IgniteUuid key = new IgniteUuid(nodeId, affKeyGen.getAndIncrement());

            if (dataCache.affinity().mapKeyToNode(key).isLocal())
                return key;
        }
    }

    /**
     * Maps affinity key to node.
     *
     * @param affinityKey Affinity key to map.
     * @return Primary node for this key.
     */
    public ClusterNode affinityNode(Object affinityKey) {
        return dataCache.affinity().mapKeyToNode(affinityKey);
    }

    /**
     * Creates new instance of explicit data loader.
     *
     * @return New instance of data loader.
     */
    private IgniteDataLoader<GridGgfsBlockKey, byte[]> dataLoader() {
        IgniteDataLoader<GridGgfsBlockKey, byte[]> ldr =
            ggfsCtx.kernalContext().<GridGgfsBlockKey, byte[]>dataLoad().dataLoader(dataCachePrj.name());

        IgniteFsConfiguration cfg = ggfsCtx.configuration();

        if (cfg.getPerNodeBatchSize() > 0)
            ldr.perNodeBufferSize(cfg.getPerNodeBatchSize());

        if (cfg.getPerNodeParallelBatchCount() > 0)
            ldr.perNodeParallelLoadOperations(cfg.getPerNodeParallelBatchCount());

        ldr.updater(GridDataLoadCacheUpdaters.<GridGgfsBlockKey, byte[]>batchedSorted());

        return ldr;
    }

    /**
     * Get list of local data blocks of the given file.
     *
     * @param fileInfo File info.
     * @return List of local data block indices.
     * @throws IgniteCheckedException If failed.
     */
    public List<Long> listLocalDataBlocks(GridGgfsFileInfo fileInfo)
        throws IgniteCheckedException {
        assert fileInfo != null;

        int prevGrpIdx = 0; // Block index within affinity group.

        boolean prevPrimaryFlag = false; // Whether previous block was primary.

        List<Long> res = new ArrayList<>();

        for (long i = 0; i < fileInfo.blocksCount(); i++) {
            // Determine group index.
            int grpIdx = (int)(i % grpSize);

            if (prevGrpIdx < grpIdx) {
                // Reuse existing affinity result.
                if (prevPrimaryFlag)
                    res.add(i);
            }
            else {
                // Re-calculate affinity result.
                GridGgfsBlockKey key = new GridGgfsBlockKey(fileInfo.id(), fileInfo.affinityKey(),
                    fileInfo.evictExclude(), i);

                Collection<ClusterNode> affNodes = dataCache.affinity().mapKeyToPrimaryAndBackups(key);

                assert affNodes != null && !affNodes.isEmpty();

                ClusterNode primaryNode = affNodes.iterator().next();

                if (primaryNode.id().equals(ggfsCtx.kernalContext().localNodeId())) {
                    res.add(i);

                    prevPrimaryFlag = true;
                }
                else
                    prevPrimaryFlag = false;
            }

            prevGrpIdx = grpIdx;
        }

        return res;
    }

    /**
     * Get data block for specified file ID and block index.
     *
     * @param fileInfo File info.
     * @param path Path reading from.
     * @param blockIdx Block index.
     * @param secReader Optional secondary file system reader.
     * @return Requested data block or {@code null} if nothing found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public IgniteInternalFuture<byte[]> dataBlock(final GridGgfsFileInfo fileInfo, final IgniteFsPath path,
        final long blockIdx, @Nullable final IgniteFsReader secReader)
        throws IgniteCheckedException {
        //assert validTxState(any); // Allow this method call for any transaction state.

        assert fileInfo != null;
        assert blockIdx >= 0;

        // Schedule block request BEFORE prefetch requests.
        final GridGgfsBlockKey key = blockKey(blockIdx, fileInfo);

        if (log.isDebugEnabled() &&
            dataCache.affinity().isPrimaryOrBackup(ggfsCtx.kernalContext().discovery().localNode(), key)) {
            log.debug("Reading non-local data block [path=" + path + ", fileInfo=" + fileInfo +
                ", blockIdx=" + blockIdx + ']');
        }

        IgniteInternalFuture<byte[]> fut = dataCachePrj.getAsync(key);

        if (secReader != null) {
            fut = fut.chain(new CX1<IgniteInternalFuture<byte[]>, byte[]>() {
                @Override public byte[] applyx(IgniteInternalFuture<byte[]> fut) throws IgniteCheckedException {
                    byte[] res = fut.get();

                    if (res == null) {
                        GridFutureAdapter<byte[]> rmtReadFut = new GridFutureAdapter<>(ggfsCtx.kernalContext());

                        IgniteInternalFuture<byte[]> oldRmtReadFut = rmtReadFuts.putIfAbsent(key, rmtReadFut);

                        if (oldRmtReadFut == null) {
                            try {
                                if (log.isDebugEnabled())
                                    log.debug("Reading non-local data block in the secondary file system [path=" +
                                        path + ", fileInfo=" + fileInfo + ", blockIdx=" + blockIdx + ']');

                                int blockSize = fileInfo.blockSize();

                                long pos = blockIdx * blockSize; // Calculate position for Hadoop

                                res = new byte[blockSize];

                                int read = 0;

                                synchronized (secReader) {
                                    try {
                                        // Delegate to the secondary file system.
                                        while (read < blockSize) {
                                            int r = secReader.read(pos + read, res, read, blockSize - read);

                                            if (r < 0)
                                                break;

                                            read += r;
                                        }
                                    }
                                    catch (IOException e) {
                                        throw new IgniteCheckedException("Failed to read data due to secondary file system " +
                                            "exception: " + e.getMessage(), e);
                                    }
                                }

                                // If we did not read full block at the end of the file - trim it.
                                if (read != blockSize)
                                    res = Arrays.copyOf(res, read);

                                rmtReadFut.onDone(res);

                                putSafe(key, res);

                                metrics.addReadBlocks(1, 1);
                            }
                            catch (IgniteCheckedException e) {
                                rmtReadFut.onDone(e);

                                throw e;
                            }
                            finally {
                                boolean rmv = rmtReadFuts.remove(key, rmtReadFut);

                                assert rmv;
                            }
                        }
                        else {
                            // Wait for existing future to finish and get it's result.
                            res = oldRmtReadFut.get();

                            metrics.addReadBlocks(1, 0);
                        }
                    }
                    else
                        metrics.addReadBlocks(1, 0);

                    return res;
                }
            });
        }
        else
            metrics.addReadBlocks(1, 0);

        return fut;
    }

    /**
     * Registers write future in ggfs data manager.
     *
     * @param fileInfo File info of file opened to write.
     * @return Future that will be completed when all ack messages are received or when write failed.
     */
    public IgniteInternalFuture<Boolean> writeStart(GridGgfsFileInfo fileInfo) {
        WriteCompletionFuture fut = new WriteCompletionFuture(ggfsCtx.kernalContext(), fileInfo.id());

        WriteCompletionFuture oldFut = pendingWrites.putIfAbsent(fileInfo.id(), fut);

        assert oldFut == null : "Opened write that is being concurrently written: " + fileInfo;

        if (log.isDebugEnabled())
            log.debug("Registered write completion future for file output stream [fileInfo=" + fileInfo +
                ", fut=" + fut + ']');

        return fut;
    }

    /**
     * Notifies data manager that no further writes will be performed on stream.
     *
     * @param fileInfo File info being written.
     */
    public void writeClose(GridGgfsFileInfo fileInfo) {
        WriteCompletionFuture fut = pendingWrites.get(fileInfo.id());

        if (fut != null)
            fut.markWaitingLastAck();
        else {
            if (log.isDebugEnabled())
                log.debug("Failed to find write completion future for file in pending write map (most likely it was " +
                    "failed): " + fileInfo);
        }
    }

    /**
     * Store data blocks in file.<br/>
     * Note! If file concurrently deleted we'll get lost blocks.
     *
     * @param fileInfo File info.
     * @param reservedLen Reserved length.
     * @param remainder Remainder.
     * @param remainderLen Remainder length.
     * @param data Data to store.
     * @param flush Flush flag.
     * @param affinityRange Affinity range to update if file write can be colocated.
     * @param batch Optional secondary file system worker batch.
     *
     * @return Remainder if data did not fill full block.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public byte[] storeDataBlocks(
        GridGgfsFileInfo fileInfo,
        long reservedLen,
        @Nullable byte[] remainder,
        int remainderLen,
        ByteBuffer data,
        boolean flush,
        GridGgfsFileAffinityRange affinityRange,
        @Nullable GridGgfsFileWorkerBatch batch
    ) throws IgniteCheckedException {
        //assert validTxState(any); // Allow this method call for any transaction state.

        return byteBufWriter.storeDataBlocks(fileInfo, reservedLen, remainder, remainderLen, data, data.remaining(),
            flush, affinityRange, batch);
    }

    /**
     * Store data blocks in file.<br/>
     * Note! If file concurrently deleted we'll got lost blocks.
     *
     * @param fileInfo File info.
     * @param reservedLen Reserved length.
     * @param remainder Remainder.
     * @param remainderLen Remainder length.
     * @param in Data to store.
     * @param len Data length to store.
     * @param flush Flush flag.
     * @param affinityRange File affinity range to update if file cal be colocated.
     * @param batch Optional secondary file system worker batch.
     * @throws IgniteCheckedException If failed.
     * @return Remainder of data that did not fit the block if {@code flush} flag is {@code false}.
     * @throws IOException If store failed.
     */
    @Nullable public byte[] storeDataBlocks(
        GridGgfsFileInfo fileInfo,
        long reservedLen,
        @Nullable byte[] remainder,
        int remainderLen,
        DataInput in,
        int len,
        boolean flush,
        GridGgfsFileAffinityRange affinityRange,
        @Nullable GridGgfsFileWorkerBatch batch
    ) throws IgniteCheckedException, IOException {
        //assert validTxState(any); // Allow this method call for any transaction state.

        return dataInputWriter.storeDataBlocks(fileInfo, reservedLen, remainder, remainderLen, in, len, flush,
            affinityRange, batch);
    }

    /**
     * Delete file's data from data cache.
     *
     * @param fileInfo File details to remove data for.
     * @return Delete future that will be completed when file is actually erased.
     */
    public IgniteInternalFuture<Object> delete(GridGgfsFileInfo fileInfo) {
        //assert validTxState(any); // Allow this method call for any transaction state.

        if (!fileInfo.isFile()) {
            if (log.isDebugEnabled())
                log.debug("Cannot delete content of not-data file: " + fileInfo);

            return new GridFinishedFuture<>(ggfsCtx.kernalContext());
        }
        else
            return delWorker.deleteAsync(fileInfo);
    }

    /**
     * @param blockIdx Block index.
     * @param fileInfo File info.
     * @return Block key.
     */
    public GridGgfsBlockKey blockKey(long blockIdx, GridGgfsFileInfo fileInfo) {
        if (fileInfo.affinityKey() != null)
            return new GridGgfsBlockKey(fileInfo.id(), fileInfo.affinityKey(), fileInfo.evictExclude(), blockIdx);

        if (fileInfo.fileMap() != null) {
            IgniteUuid affKey = fileInfo.fileMap().affinityKey(blockIdx * fileInfo.blockSize(), false);

            return new GridGgfsBlockKey(fileInfo.id(), affKey, fileInfo.evictExclude(), blockIdx);
        }

        return new GridGgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), blockIdx);
    }

    /**
     * Tries to remove blocks affected by fragmentizer. If {@code cleanNonColocated} is {@code true}, will remove
     * non-colocated blocks as well.
     *
     * @param fileInfo File info to clean up.
     * @param range Range to clean up.
     * @param cleanNonColocated {@code True} if all blocks should be cleaned.
     */
    public void cleanBlocks(GridGgfsFileInfo fileInfo, GridGgfsFileAffinityRange range, boolean cleanNonColocated) {
        long startIdx = range.startOffset() / fileInfo.blockSize();

        long endIdx = range.endOffset() / fileInfo.blockSize();

        if (log.isDebugEnabled())
            log.debug("Cleaning blocks [fileInfo=" + fileInfo + ", range=" + range +
                ", cleanNonColocated=" + cleanNonColocated + ", startIdx=" + startIdx + ", endIdx=" + endIdx + ']');

        try {
            try (IgniteDataLoader<GridGgfsBlockKey, byte[]> ldr = dataLoader()) {
                for (long idx = startIdx; idx <= endIdx; idx++) {
                    ldr.removeData(new GridGgfsBlockKey(fileInfo.id(), range.affinityKey(), fileInfo.evictExclude(),
                        idx));

                    if (cleanNonColocated)
                        ldr.removeData(new GridGgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), idx));
                }
            }
        }
        catch (IgniteException e) {
            log.error("Failed to clean up file range [fileInfo=" + fileInfo + ", range=" + range + ']', e);
        }
    }

    /**
     * Moves all colocated blocks in range to non-colocated keys.
     * @param fileInfo File info to move data for.
     * @param range Range to move.
     */
    public void spreadBlocks(GridGgfsFileInfo fileInfo, GridGgfsFileAffinityRange range) {
        long startIdx = range.startOffset() / fileInfo.blockSize();

        long endIdx = range.endOffset() / fileInfo.blockSize();

        try {
            try (IgniteDataLoader<GridGgfsBlockKey, byte[]> ldr = dataLoader()) {
                long bytesProcessed = 0;

                for (long idx = startIdx; idx <= endIdx; idx++) {
                    GridGgfsBlockKey colocatedKey = new GridGgfsBlockKey(fileInfo.id(), range.affinityKey(),
                        fileInfo.evictExclude(), idx);

                    GridGgfsBlockKey key = new GridGgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), idx);

                    // Most of the time should be local get.
                    byte[] block = dataCachePrj.get(colocatedKey);

                    if (block != null) {
                        // Need to check if block is partially written.
                        // If so, must update it in pessimistic transaction.
                        if (block.length != fileInfo.blockSize()) {
                            try (IgniteInternalTx tx = dataCachePrj.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                                Map<GridGgfsBlockKey, byte[]> vals = dataCachePrj.getAll(F.asList(colocatedKey, key));

                                byte[] val = vals.get(colocatedKey);

                                if (val != null) {
                                    dataCachePrj.putx(key, val);

                                    tx.commit();
                                }
                                else {
                                    // File is being concurrently deleted.
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to find colocated file block for spread (will ignore) " +
                                            "[fileInfo=" + fileInfo + ", range=" + range + ", startIdx=" + startIdx +
                                            ", endIdx=" + endIdx + ", idx=" + idx + ']');
                                }
                            }
                        }
                        else
                            ldr.addData(key, block);

                        bytesProcessed += block.length;

                        if (bytesProcessed >= ggfsCtx.configuration().getFragmentizerThrottlingBlockLength()) {
                            ldr.flush();

                            bytesProcessed = 0;

                            U.sleep(ggfsCtx.configuration().getFragmentizerThrottlingDelay());
                        }
                    }
                    else if (log.isDebugEnabled())
                        log.debug("Failed to find colocated file block for spread (will ignore) " +
                            "[fileInfo=" + fileInfo + ", range=" + range + ", startIdx=" + startIdx +
                            ", endIdx=" + endIdx + ", idx=" + idx + ']');
                }
            }
        }
        catch (IgniteCheckedException e) {
            log.error("Failed to clean up file range [fileInfo=" + fileInfo + ", range=" + range + ']', e);
        }
    }

    /**
     * Resolve affinity nodes for specified part of file.
     *
     * @param info File info to resolve affinity nodes for.
     * @param start Start position in the file.
     * @param len File part length to get affinity for.
     * @return Affinity blocks locations.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<IgniteFsBlockLocation> affinity(GridGgfsFileInfo info, long start, long len)
        throws IgniteCheckedException {
        return affinity(info, start, len, 0);
    }

    /**
     * Resolve affinity nodes for specified part of file.
     *
     * @param info File info to resolve affinity nodes for.
     * @param start Start position in the file.
     * @param len File part length to get affinity for.
     * @param maxLen Maximum block length.
     * @return Affinity blocks locations.
     * @throws IgniteCheckedException If failed.
     */
    public Collection<IgniteFsBlockLocation> affinity(GridGgfsFileInfo info, long start, long len, long maxLen)
        throws IgniteCheckedException {
        assert validTxState(false);
        assert info.isFile() : "Failed to get affinity (not a file): " + info;
        assert start >= 0 : "Start position should not be negative: " + start;
        assert len >= 0 : "Part length should not be negative: " + len;

        if (log.isDebugEnabled())
            log.debug("Calculating affinity for file [info=" + info + ", start=" + start + ", len=" + len + ']');

        // Skip affinity resolving, if no data requested.
        if (len == 0)
            return Collections.emptyList();

        if (maxLen > 0) {
            maxLen -= maxLen % info.blockSize();

            // If maxLen is smaller than block size, then adjust it to the block size.
            if (maxLen < info.blockSize())
                maxLen = info.blockSize();
        }
        else
            maxLen = 0;

        // In case when affinity key is not null the whole file resides on one node.
        if (info.affinityKey() != null) {
            Collection<IgniteFsBlockLocation> res = new LinkedList<>();

            splitBlocks(start, len, maxLen, dataCache.affinity().mapKeyToPrimaryAndBackups(
                new GridGgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(), 0)), res);

            return res;
        }

        // Need to merge ranges affinity with non-colocated affinity.
        Deque<IgniteFsBlockLocation> res = new LinkedList<>();

        if (info.fileMap().ranges().isEmpty()) {
            affinity0(info, start, len, maxLen, res);

            return res;
        }

        long pos = start;
        long end = start + len;

        for (GridGgfsFileAffinityRange range : info.fileMap().ranges()) {
            if (log.isDebugEnabled())
                log.debug("Checking range [range=" + range + ", pos=" + pos + ']');

            // If current position is less than range start, add non-colocated affinity ranges.
            if (range.less(pos)) {
                long partEnd = Math.min(end, range.startOffset());

                affinity0(info, pos, partEnd - pos, maxLen, res);

                pos = partEnd;
            }

            IgniteFsBlockLocation last = res.peekLast();

            if (range.belongs(pos)) {
                long partEnd = Math.min(range.endOffset() + 1, end);

                Collection<ClusterNode> affNodes = dataCache.affinity().mapKeyToPrimaryAndBackups(
                    range.affinityKey());

                if (log.isDebugEnabled())
                    log.debug("Calculated affinity for range [start=" + pos + ", end=" + partEnd +
                        ", nodes=" + F.nodeIds(affNodes) + ", range=" + range +
                        ", affNodes=" + F.nodeIds(affNodes) + ']');

                if (last != null && equal(last.nodeIds(), F.viewReadOnly(affNodes, F.node2id()))) {
                    // Merge with the previous block in result.
                    res.removeLast();

                    splitBlocks(last.start(), last.length() + partEnd - pos, maxLen, affNodes, res);
                }
                else
                    // Do not merge with the previous block.
                    splitBlocks(pos, partEnd - pos, maxLen, affNodes, res);

                pos = partEnd;
            }
            // Else skip this range.

            if (log.isDebugEnabled())
                log.debug("Finished range check [range=" + range + ", pos=" + pos + ", res=" + res + ']');

            if (pos == end)
                break;
        }

        // Final chunk.
        if (pos != end)
            affinity0(info, pos, end, maxLen, res);

        return res;
    }

    /**
     * Calculates non-colocated affinity for given file info and given region of file.
     *
     * @param info File info.
     * @param start Start offset.
     * @param len Length.
     * @param maxLen Maximum allowed split length.
     * @param res Result collection to add regions to.
     * @throws IgniteCheckedException If failed.
     */
    private void affinity0(GridGgfsFileInfo info, long start, long len, long maxLen, Deque<IgniteFsBlockLocation> res)
        throws IgniteCheckedException {
        long firstGrpIdx = start / grpBlockSize;
        long limitGrpIdx = (start + len + grpBlockSize - 1) / grpBlockSize;

        if (limitGrpIdx - firstGrpIdx > Integer.MAX_VALUE)
            throw new IgniteFsException("Failed to get affinity (range is too wide)" +
                " [info=" + info + ", start=" + start + ", len=" + len + ']');

        if (log.isDebugEnabled())
            log.debug("Mapping file region [fileInfo=" + info + ", start=" + start + ", len=" + len + ']');

        for (long grpIdx = firstGrpIdx; grpIdx < limitGrpIdx; grpIdx++) {
            // Boundaries of the block.
            long blockStart;
            long blockLen;

            // The first block.
            if (grpIdx == firstGrpIdx) {
                blockStart = start % grpBlockSize;
                blockLen = Math.min(grpBlockSize - blockStart, len);
            }
            // The last block.
            else if (grpIdx == limitGrpIdx - 1) {
                blockStart = 0;
                blockLen = (start + len - 1) % grpBlockSize + 1;
            }
            // Other blocks.
            else {
                blockStart = 0;
                blockLen = grpBlockSize;
            }

            // Affinity for the first block in the group.
            GridGgfsBlockKey key = new GridGgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(),
                grpIdx * grpSize);

            Collection<ClusterNode> affNodes = dataCache.affinity().mapKeyToPrimaryAndBackups(key);

            if (log.isDebugEnabled())
                log.debug("Mapped key to nodes [key=" + key + ", nodes=" + F.nodeIds(affNodes) +
                ", blockStart=" + blockStart + ", blockLen=" + blockLen + ']');

            IgniteFsBlockLocation last = res.peekLast();

            // Merge with previous affinity block location?
            if (last != null && equal(last.nodeIds(), F.viewReadOnly(affNodes, F.node2id()))) {
                // Remove previous incomplete value.
                res.removeLast();

                // Update affinity block location with merged one.
                splitBlocks(last.start(), last.length() + blockLen, maxLen, affNodes, res);
            }
            else
                splitBlocks(grpIdx * grpBlockSize + blockStart, blockLen, maxLen, affNodes, res);
        }

        if (log.isDebugEnabled())
            log.debug("Calculated file affinity [info=" + info + ", start=" + start + ", len=" + len +
                ", res=" + res + ']');
    }

    /**
     * Split blocks according to maximum split length.
     *
     * @param start Start position.
     * @param len Length.
     * @param maxLen Maximum allowed length.
     * @param nodes Affinity nodes.
     * @param res Where to put results.
     */
    private void splitBlocks(long start, long len, long maxLen,
        Collection<ClusterNode> nodes, Collection<IgniteFsBlockLocation> res) {
        if (maxLen > 0) {
            long end = start + len;

            long start0 = start;

            while (start0 < end) {
                long len0 = Math.min(maxLen, end - start0);

                res.add(new GridGgfsBlockLocationImpl(start0, len0, nodes));

                start0 += len0;
            }
        }
        else
            res.add(new GridGgfsBlockLocationImpl(start, len, nodes));
    }

    /**
     * Gets group block size (block size * group size).
     *
     * @return Group block size.
     */
    public long groupBlockSize() {
        return grpBlockSize;
    }

    /**
     * Check if two collections are equal as if they are lists (with respect to order).
     *
     * @param one First collection.
     * @param two Second collection.
     * @return {@code True} if equal.
     */
    private boolean equal(Collection<UUID> one, Collection<UUID> two) {
        if (one.size() != two.size())
            return false;

        Iterator<UUID> it1 = one.iterator();
        Iterator<UUID> it2 = two.iterator();

        int size = one.size();

        for (int i = 0; i < size; i++) {
            if (!it1.next().equals(it2.next()))
                return false;
        }

        return true;
    }

    /**
     * Check transaction is (not) started.
     *
     * @param inTx Expected transaction state.
     * @return Transaction state is correct.
     */
    private boolean validTxState(boolean inTx) {
        boolean txState = inTx == (dataCachePrj.tx() != null);

        assert txState : (inTx ? "Method cannot be called outside transaction: " :
            "Method cannot be called in transaction: ") + dataCachePrj.tx();

        return txState;
    }

    /**
     * @param fileId File ID.
     * @param node Node to process blocks on.
     * @param blocks Blocks to put in cache.
     * @throws IgniteCheckedException If batch processing failed.
     */
    private void processBatch(IgniteUuid fileId, final ClusterNode node,
        final Map<GridGgfsBlockKey, byte[]> blocks) throws IgniteCheckedException {
        final long batchId = reqIdCtr.getAndIncrement();

        final WriteCompletionFuture completionFut = pendingWrites.get(fileId);

        if (completionFut == null) {
            if (log.isDebugEnabled())
                log.debug("Missing completion future for file write request (most likely exception occurred " +
                    "which will be thrown upon stream close) [nodeId=" + node.id() + ", fileId=" + fileId + ']');

            return;
        }

        // Throw exception if future is failed in the middle of writing.
        if (completionFut.isDone())
            completionFut.get();

        completionFut.onWriteRequest(node.id(), batchId);

        final UUID nodeId = node.id();

        if (!node.isLocal()) {
            final GridGgfsBlocksMessage msg = new GridGgfsBlocksMessage(fileId, batchId, blocks);

            callGgfsLocalSafe(new GridPlainCallable<Object>() {
                @Override @Nullable public Object call() throws Exception {
                    try {
                        ggfsCtx.send(nodeId, topic, msg, SYSTEM_POOL);
                    }
                    catch (IgniteCheckedException e) {
                        completionFut.onError(nodeId, e);
                    }

                    return null;
                }
            });
        }
        else {
            callGgfsLocalSafe(new GridPlainCallable<Object>() {
                @Override @Nullable public Object call() throws Exception {
                    storeBlocksAsync(blocks).listenAsync(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> fut) {
                            try {
                                fut.get();

                                completionFut.onWriteAck(nodeId, batchId);
                            }
                            catch (IgniteCheckedException e) {
                                completionFut.onError(nodeId, e);
                            }
                        }
                    });

                    return null;
                }
            });
        }
    }

    /**
     * If partial block write is attempted, both colocated and non-colocated keys are locked and data is appended
     * to correct block.
     *
     * @param fileId File ID.
     * @param colocatedKey Block key.
     * @param startOff Data start offset within block.
     * @param data Data to write.
     * @throws IgniteCheckedException If update failed.
     */
    private void processPartialBlockWrite(IgniteUuid fileId, GridGgfsBlockKey colocatedKey, int startOff,
        byte[] data) throws IgniteCheckedException {
        if (dataCachePrj.ggfsDataSpaceUsed() >= dataCachePrj.ggfsDataSpaceMax()) {
            try {
                ggfs.awaitDeletesAsync().get(trashPurgeTimeout);
            }
            catch (IgniteFutureTimeoutCheckedException ignore) {
                // Ignore.
            }

            // Additional size check.
            if (dataCachePrj.ggfsDataSpaceUsed() >= dataCachePrj.ggfsDataSpaceMax()) {
                final WriteCompletionFuture completionFut = pendingWrites.get(fileId);

                if (completionFut == null) {
                    if (log.isDebugEnabled())
                        log.debug("Missing completion future for file write request (most likely exception occurred " +
                            "which will be thrown upon stream close) [fileId=" + fileId + ']');

                    return;
                }

                IgniteFsOutOfSpaceException e = new IgniteFsOutOfSpaceException("Failed to write data block " +
                    "(GGFS maximum data size exceeded) [used=" + dataCachePrj.ggfsDataSpaceUsed() +
                    ", allowed=" + dataCachePrj.ggfsDataSpaceMax() + ']');

                completionFut.onDone(new IgniteCheckedException("Failed to write data (not enough space on node): " +
                    ggfsCtx.kernalContext().localNodeId(), e));

                return;
            }
        }

        // No affinity key present, just concat and return.
        if (colocatedKey.affinityKey() == null) {
            dataCachePrj.invoke(colocatedKey, new UpdateProcessor(startOff, data));

            return;
        }

        // If writing from block beginning, just put and return.
        if (startOff == 0) {
            dataCachePrj.putx(colocatedKey, data);

            return;
        }

        // Create non-colocated key.
        GridGgfsBlockKey key = new GridGgfsBlockKey(colocatedKey.getFileId(), null,
            colocatedKey.evictExclude(), colocatedKey.getBlockId());

        try (IgniteInternalTx tx = dataCachePrj.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
            // Lock keys.
            Map<GridGgfsBlockKey, byte[]> vals = dataCachePrj.getAll(F.asList(colocatedKey, key));

            boolean hasVal = false;

            UpdateProcessor transformClos = new UpdateProcessor(startOff, data);

            if (vals.get(colocatedKey) != null) {
                dataCachePrj.invoke(colocatedKey, transformClos);

                hasVal = true;
            }

            if (vals.get(key) != null) {
                dataCachePrj.invoke(key, transformClos);

                hasVal = true;
            }

            if (!hasVal)
                throw new IgniteCheckedException("Failed to write partial block (no previous data was found in cache) " +
                    "[key=" + colocatedKey + ", relaxedKey=" + key + ", startOff=" + startOff +
                    ", dataLen=" + data.length + ']');

            tx.commit();
        }
    }

    /**
     * Executes callable in GGFS executor service. If execution rejected, callable will be executed
     * in caller thread.
     *
     * @param c Callable to execute.
     */
    private <T> void callGgfsLocalSafe(Callable<T> c) {
        try {
            ggfsSvc.submit(c);
        }
        catch (RejectedExecutionException ignored) {
            // This exception will happen if network speed is too low and data comes faster
            // than we can send it to remote nodes.
            try {
                c.call();
            }
            catch (Exception e) {
                log.warning("Failed to execute GGFS callable: " + c, e);
            }
        }
    }

    /**
     * Put data block read from the secondary file system to the cache.
     *
     * @param key Key.
     * @param data Data.
     * @throws IgniteCheckedException If failed.
     */
    private void putSafe(final GridGgfsBlockKey key, final byte[] data) throws IgniteCheckedException {
        assert key != null;
        assert data != null;

        if (maxPendingPuts > 0) {
            pendingPutsLock.lock();

            try {
                while (curPendingPuts > maxPendingPuts)
                    pendingPutsCond.await(2000, TimeUnit.MILLISECONDS);

                curPendingPuts += data.length;
            }
            catch (InterruptedException ignore) {
                throw new IgniteCheckedException("Failed to put GGFS data block into cache due to interruption: " + key);
            }
            finally {
                pendingPutsLock.unlock();
            }
        }

        Runnable task = new Runnable() {
            @Override public void run() {
                try {
                    dataCachePrj.putx(key, data);
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Failed to put GGFS data block into cache [key=" + key + ", err=" + e + ']');
                }
                finally {
                    if (maxPendingPuts > 0) {
                        pendingPutsLock.lock();

                        try {
                            curPendingPuts -= data.length;

                            pendingPutsCond.signalAll();
                        }
                        finally {
                            pendingPutsLock.unlock();
                        }
                    }
                }
            }
        };

        try {
            putExecSvc.submit(task);
        }
        catch (RejectedExecutionException ignore) {
            task.run();
        }
    }

    /**
     * @param blocks Blocks to write.
     * @return Future that will be completed after put is done.
     */
    @SuppressWarnings("unchecked")
    private IgniteInternalFuture<?> storeBlocksAsync(Map<GridGgfsBlockKey, byte[]> blocks) {
        assert !blocks.isEmpty();

        if (dataCachePrj.ggfsDataSpaceUsed() >= dataCachePrj.ggfsDataSpaceMax()) {
            try {
                try {
                    ggfs.awaitDeletesAsync().get(trashPurgeTimeout);
                }
                catch (IgniteFutureTimeoutCheckedException ignore) {
                    // Ignore.
                }

                // Additional size check.
                if (dataCachePrj.ggfsDataSpaceUsed() >= dataCachePrj.ggfsDataSpaceMax())
                    return new GridFinishedFuture<Object>(ggfsCtx.kernalContext(),
                        new IgniteFsOutOfSpaceException("Failed to write data block (GGFS maximum data size " +
                            "exceeded) [used=" + dataCachePrj.ggfsDataSpaceUsed() +
                            ", allowed=" + dataCachePrj.ggfsDataSpaceMax() + ']'));

            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(ggfsCtx.kernalContext(), new IgniteCheckedException("Failed to store data " +
                    "block due to unexpected exception.", e));
            }
        }

        return dataCachePrj.putAllAsync(blocks);
    }

    /**
     * @param nodeId Node ID.
     * @param blocksMsg Write request message.
     */
    private void processBlocksMessage(final UUID nodeId, final GridGgfsBlocksMessage blocksMsg) {
        storeBlocksAsync(blocksMsg.blocks()).listenAsync(new CI1<IgniteInternalFuture<?>>() {
            @Override public void apply(IgniteInternalFuture<?> fut) {
                IgniteCheckedException err = null;

                try {
                    fut.get();
                }
                catch (IgniteCheckedException e) {
                    err = e;
                }

                try {
                    // Send reply back to node.
                    ggfsCtx.send(nodeId, topic, new GridGgfsAckMessage(blocksMsg.fileId(), blocksMsg.id(), err),
                        SYSTEM_POOL);
                }
                catch (IgniteCheckedException e) {
                    U.warn(log, "Failed to send batch acknowledgement (did node leave the grid?) [nodeId=" + nodeId +
                        ", fileId=" + blocksMsg.fileId() + ", batchId=" + blocksMsg.id() + ']', e);
                }
            }
        });
    }

    /**
     * @param nodeId Node ID.
     * @param ackMsg Write acknowledgement message.
     */
    private void processAckMessage(UUID nodeId, GridGgfsAckMessage ackMsg) {
        try {
            ackMsg.finishUnmarshal(ggfsCtx.kernalContext().config().getMarshaller(), null);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to unmarshal message (will ignore): " + ackMsg, e);

            return;
        }

        IgniteUuid fileId = ackMsg.fileId();

        WriteCompletionFuture fut = pendingWrites.get(fileId);

        if (fut != null) {
            if (ackMsg.error() != null)
                fut.onError(nodeId, ackMsg.error());
            else
                fut.onWriteAck(nodeId, ackMsg.id());
        }
        else {
            if (log.isDebugEnabled())
                log.debug("Received write acknowledgement for non-existent write future (most likely future was " +
                    "failed) [nodeId=" + nodeId + ", fileId=" + fileId + ']');
        }
    }

    /**
     * Creates block key based on block ID, file info and local affinity range.
     *
     * @param block Block ID.
     * @param fileInfo File info being written.
     * @param locRange Local affinity range to update.
     * @return Block key.
     */
    private GridGgfsBlockKey createBlockKey(
        long block,
        GridGgfsFileInfo fileInfo,
        GridGgfsFileAffinityRange locRange
    ) {
        // If affinityKey is present, return block key as is.
        if (fileInfo.affinityKey() != null)
            return new GridGgfsBlockKey(fileInfo.id(), fileInfo.affinityKey(), fileInfo.evictExclude(), block);

        // If range is done, no colocated writes are attempted.
        if (locRange == null || locRange.done())
            return new GridGgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), block);

        long blockStart = block * fileInfo.blockSize();

        // If block does not belong to new range, return old affinity key.
        if (locRange.less(blockStart)) {
            IgniteUuid affKey = fileInfo.fileMap().affinityKey(blockStart, false);

            return new GridGgfsBlockKey(fileInfo.id(), affKey, fileInfo.evictExclude(), block);
        }

        // Check if we have enough free space to do colocated writes.
        if (dataCachePrj.ggfsDataSpaceUsed() > dataCachePrj.ggfsDataSpaceMax() *
            ggfsCtx.configuration().getFragmentizerLocalWritesRatio()) {
            // Forbid further co-location.
            locRange.markDone();

            return new GridGgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), block);
        }

        if (!locRange.belongs(blockStart))
            locRange.expand(blockStart, fileInfo.blockSize());

        return new GridGgfsBlockKey(fileInfo.id(), locRange.affinityKey(), fileInfo.evictExclude(), block);
    }

    /**
     * Abstract class to handle writes from different type of input data.
     */
    private abstract class BlocksWriter<T> {
        /**
         * Stores data blocks read from abstracted source.
         *
         * @param fileInfo File info.
         * @param reservedLen Reserved length.
         * @param remainder Remainder.
         * @param remainderLen Remainder length.
         * @param src Source to read bytes.
         * @param srcLen Data length to read from source.
         * @param flush Flush flag.
         * @param affinityRange Affinity range to update if file write can be colocated.
         * @param batch Optional secondary file system worker batch.
         * @throws IgniteCheckedException If failed.
         * @return Data remainder if {@code flush} flag is {@code false}.
         */
        @Nullable public byte[] storeDataBlocks(
            GridGgfsFileInfo fileInfo,
            long reservedLen,
            @Nullable byte[] remainder,
            final int remainderLen,
            T src,
            int srcLen,
            boolean flush,
            GridGgfsFileAffinityRange affinityRange,
            @Nullable GridGgfsFileWorkerBatch batch
        ) throws IgniteCheckedException {
            IgniteUuid id = fileInfo.id();
            int blockSize = fileInfo.blockSize();

            int len = remainderLen + srcLen;

            if (len > reservedLen)
                throw new IgniteFsException("Not enough space reserved to store data [id=" + id +
                    ", reservedLen=" + reservedLen + ", remainderLen=" + remainderLen +
                    ", data.length=" + srcLen + ']');

            long start = reservedLen - len;
            long first = start / blockSize;
            long limit = (start + len + blockSize - 1) / blockSize;
            int written = 0;
            int remainderOff = 0;

            Map<GridGgfsBlockKey, byte[]> nodeBlocks = U.newLinkedHashMap((int)(limit - first));
            ClusterNode node = null;
            int off = 0;

            for (long block = first; block < limit; block++) {
                final long blockStartOff = block == first ? (start % blockSize) : 0;
                final long blockEndOff = block == (limit - 1) ? (start + len - 1) % blockSize : (blockSize - 1);

                final long size = blockEndOff - blockStartOff + 1;

                assert size > 0 && size <= blockSize;
                assert blockStartOff + size <= blockSize;

                final byte[] portion = new byte[(int)size];

                // Data length to copy from remainder.
                int portionOff = Math.min((int)size, remainderLen - remainderOff);

                if (remainderOff != remainderLen) {
                    U.arrayCopy(remainder, remainderOff, portion, 0, portionOff);

                    remainderOff += portionOff;
                }

                if (portionOff < size)
                    readData(src, portion, portionOff);

                // Will update range if necessary.
                GridGgfsBlockKey key = createBlockKey(block, fileInfo, affinityRange);

                ClusterNode primaryNode = dataCachePrj.cache().affinity().mapKeyToNode(key);

                if (block == first) {
                    off = (int)blockStartOff;
                    node = primaryNode;
                }

                if (size == blockSize) {
                    assert blockStartOff == 0 : "Cannot write the whole block not from start position [start=" +
                        start + ", block=" + block + ", blockStartOff=" + blockStartOff + ", blockEndOff=" +
                        blockEndOff + ", size=" + size + ", first=" + first + ", limit=" + limit + ", blockSize=" +
                        blockSize + ']';
                }
                else {
                    // If partial block is being written from the beginning and not flush, return it as remainder.
                    if (blockStartOff == 0 && !flush) {
                        assert written + portion.length == len;

                        if (!nodeBlocks.isEmpty()) {
                            processBatch(id, node, nodeBlocks);

                            metrics.addWriteBlocks(1, 0);
                        }

                        return portion;
                    }
                }

                int writtenSecondary = 0;

                if (batch != null) {
                    if (!batch.write(portion))
                        throw new IgniteCheckedException("Cannot write more data to the secondary file system output " +
                            "stream because it was marked as closed: " + batch.path());
                    else
                        writtenSecondary = 1;
                }

                assert primaryNode != null;

                int writtenTotal = 0;

                if (!primaryNode.id().equals(node.id())) {
                    if (!nodeBlocks.isEmpty())
                        processBatch(id, node, nodeBlocks);

                    writtenTotal = nodeBlocks.size();

                    nodeBlocks = U.newLinkedHashMap((int)(limit - first));
                    node = primaryNode;
                }

                assert size == portion.length;

                if (size != blockSize) {
                    // Partial writes must be always synchronous.
                    processPartialBlockWrite(id, key, block == first ? off : 0, portion);

                    writtenTotal++;
                }
                else
                    nodeBlocks.put(key, portion);

                metrics.addWriteBlocks(writtenTotal, writtenSecondary);

                written += portion.length;
            }

            // Process final batch, if exists.
            if (!nodeBlocks.isEmpty()) {
                processBatch(id, node, nodeBlocks);

                metrics.addWriteBlocks(nodeBlocks.size(), 0);
            }

            assert written == len;

            return null;
        }

        /**
         * Fully reads data from specified source into the specified byte array.
         *
         * @param src Data source.
         * @param dst Destination.
         * @param dstOff Destination buffer offset.
         * @throws IgniteCheckedException If read failed.
         */
        protected abstract void readData(T src, byte[] dst, int dstOff) throws IgniteCheckedException;
    }

    /**
     * Byte buffer writer.
     */
    private class ByteBufferBlocksWriter extends BlocksWriter<ByteBuffer> {
        /** {@inheritDoc} */
        @Override protected void readData(ByteBuffer src, byte[] dst, int dstOff) {
            src.get(dst, dstOff, dst.length - dstOff);
        }
    }

    /**
     * Data input writer.
     */
    private class DataInputBlocksWriter extends BlocksWriter<DataInput> {
        /** {@inheritDoc} */
        @Override protected void readData(DataInput src, byte[] dst, int dstOff)
            throws IgniteCheckedException {
            try {
                src.readFully(dst, dstOff, dst.length - dstOff);
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
        }
    }

    /**
     * Helper closure to update data in cache.
     */
    @GridInternal
    private static final class UpdateProcessor implements EntryProcessor<GridGgfsBlockKey, byte[], Void>,
        Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Start position in the block to write new data from. */
        private int start;

        /** Data block to write into cache. */
        private byte[] data;

        /**
         * Empty constructor required for {@link Externalizable}.
         *
         */
        public UpdateProcessor() {
            // No-op.
        }

        /**
         * Constructs update data block closure.
         *
         * @param start Start position in the block to write new data from.
         * @param data Data block to write into cache.
         */
        private UpdateProcessor(int start, byte[] data) {
            assert start >= 0;
            assert data != null;
            assert start + data.length >= 0 : "Too much data [start=" + start + ", data.length=" + data.length + ']';

            this.start = start;
            this.data = data;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<GridGgfsBlockKey, byte[]> entry, Object... args) {
            byte[] e = entry.getValue();

            final int size = data.length;

            if (e == null || e.length == 0)
                e = new byte[start + size]; // Don't allocate more, then required.
            else if (e.length < start + size) {
                // Expand stored data array, if it less, then required.
                byte[] tmp = new byte[start + size]; // Don't allocate more than required.

                U.arrayCopy(e, 0, tmp, 0, e.length);

                e = tmp;
            }

            // Copy data into entry.
            U.arrayCopy(data, 0, e, start, size);

            entry.setValue(e);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(start);
            U.writeByteArray(out, data);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException {
            start = in.readInt();
            data = U.readByteArray(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(UpdateProcessor.class, this, "start", start, "data.length", data.length);
        }
    }

    /**
     * Asynchronous delete worker.
     */
    private class AsyncDeleteWorker extends GridWorker {
        /** File info for stop request. */
        private final GridGgfsFileInfo stopInfo = new GridGgfsFileInfo();

        /** Delete requests queue. */
        private BlockingQueue<IgniteBiTuple<GridFutureAdapter<Object>, GridGgfsFileInfo>> delReqs =
            new LinkedBlockingQueue<>();

        /**
         * @param gridName Grid name.
         * @param name Worker name.
         * @param log Log.
         */
        protected AsyncDeleteWorker(@Nullable String gridName, String name, IgniteLogger log) {
            super(gridName, name, log);
        }

        /**
         * Gracefully stops worker by adding STOP_INFO to queue.
         */
        private void stop() {
            delReqs.offer(F.t(new GridFutureAdapter<>(ggfsCtx.kernalContext()), stopInfo));
        }

        /**
         * @param info File info to delete.
         * @return Future which completes when entry is actually removed.
         */
        private IgniteInternalFuture<Object> deleteAsync(GridGgfsFileInfo info) {
            GridFutureAdapter<Object> fut = new GridFutureAdapter<>(ggfsCtx.kernalContext());

            delReqs.offer(F.t(fut, info));

            return fut;
        }

        /** {@inheritDoc} */
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                while (!isCancelled()) {
                    IgniteBiTuple<GridFutureAdapter<Object>, GridGgfsFileInfo> req = delReqs.take();

                    GridFutureAdapter<Object> fut = req.get1();
                    GridGgfsFileInfo fileInfo = req.get2();

                    // Identity check.
                    if (fileInfo == stopInfo) {
                        fut.onDone();

                        break;
                    }

                    IgniteDataLoader<GridGgfsBlockKey, byte[]> ldr = dataLoader();

                    try {
                        GridGgfsFileMap map = fileInfo.fileMap();

                        for (long block = 0, size = fileInfo.blocksCount(); block < size; block++) {
                            IgniteUuid affKey = map == null ? null : map.affinityKey(block * fileInfo.blockSize(), true);

                            ldr.removeData(new GridGgfsBlockKey(fileInfo.id(), affKey, fileInfo.evictExclude(),
                                block));

                            if (affKey != null)
                                ldr.removeData(new GridGgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(),
                                    block));
                        }
                    }
                    catch (IgniteInterruptedException ignored) {
                        // Ignore interruption during shutdown.
                    }
                    catch (IgniteException e) {
                        log.error("Failed to remove file contents: " + fileInfo, e);
                    }
                    finally {
                        try {
                            IgniteUuid fileId = fileInfo.id();

                            for (long block = 0, size = fileInfo.blocksCount(); block < size; block++)
                                ldr.removeData(new GridGgfsBlockKey(fileId, fileInfo.affinityKey(),
                                    fileInfo.evictExclude(), block));
                        }
                        catch (IgniteException e) {
                            log.error("Failed to remove file contents: " + fileInfo, e);
                        }
                        finally {
                            try {
                                ldr.close(isCancelled());
                            }
                            catch (IgniteException e) {
                                log.error("Failed to stop data loader while shutting down ggfs async delete thread.", e);
                            }
                            finally {
                                fut.onDone(); // Complete future.
                            }
                        }
                    }
                }
            }
            finally {
                if (log.isDebugEnabled())
                    log.debug("Stopping asynchronous ggfs file delete thread: " + name());

                IgniteBiTuple<GridFutureAdapter<Object>, GridGgfsFileInfo> req = delReqs.poll();

                while (req != null) {
                    req.get1().onCancelled();

                    req = delReqs.poll();
                }
            }
        }
    }

    /**
     * Future that is completed when all participating
     */
    private class WriteCompletionFuture extends GridFutureAdapter<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** File id to remove future from map. */
        private IgniteUuid fileId;

        /** Pending acks. */
        private ConcurrentMap<UUID, Set<Long>> pendingAcks = new ConcurrentHashMap8<>();

        /** Flag indicating future is waiting for last ack. */
        private volatile boolean awaitingLast;

        /**
         * Empty constructor required by {@link Externalizable}.
         */
        public WriteCompletionFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         * @param fileId File id.
         */
        private WriteCompletionFuture(GridKernalContext ctx, IgniteUuid fileId) {
            super(ctx);

            assert fileId != null;

            this.fileId = fileId;
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Boolean res, @Nullable Throwable err) {
            if (!isDone()) {
                pendingWrites.remove(fileId, this);

                if (super.onDone(res, err))
                    return true;
            }

            return false;
        }

        /**
         * Write request will be asynchronously executed on node with given ID.
         *
         * @param nodeId Node ID.
         * @param batchId Assigned batch ID.
         */
        private void onWriteRequest(UUID nodeId, long batchId) {
            if (!isDone()) {
                Set<Long> reqIds = pendingAcks.get(nodeId);

                if (reqIds == null)
                    reqIds = F.addIfAbsent(pendingAcks, nodeId, new GridConcurrentHashSet<Long>());

                reqIds.add(batchId);
            }
        }

        /**
         * Error occurred on node with given ID.
         *
         * @param nodeId Node ID.
         * @param e Caught exception.
         */
        private void onError(UUID nodeId, IgniteCheckedException e) {
            Set<Long> reqIds = pendingAcks.get(nodeId);

            // If waiting for ack from this node.
            if (reqIds != null && !reqIds.isEmpty()) {
                if (e.hasCause(IgniteFsOutOfSpaceException.class))
                    onDone(new IgniteCheckedException("Failed to write data (not enough space on node): " + nodeId, e));
                else
                    onDone(new IgniteCheckedException(
                        "Failed to wait for write completion (write failed on node): " + nodeId, e));
            }
        }

        /**
         * Write ack received from node with given ID for given batch ID.
         *
         * @param nodeId Node ID.
         * @param batchId Batch ID.
         */
        private void onWriteAck(UUID nodeId, long batchId) {
            if (!isDone()) {
                Set<Long> reqIds = pendingAcks.get(nodeId);

                assert reqIds != null : "Received acknowledgement message for not registered node [nodeId=" +
                    nodeId + ", batchId=" + batchId + ']';

                boolean rmv = reqIds.remove(batchId);

                assert rmv : "Received acknowledgement message for not registered batch [nodeId=" +
                    nodeId + ", batchId=" + batchId + ']';

                if (awaitingLast && checkCompleted())
                    onDone(true);
            }
        }

        /**
         * Marks this future as waiting last ack.
         */
        private void markWaitingLastAck() {
            awaitingLast = true;

            if (log.isDebugEnabled())
                log.debug("Marked write completion future as awaiting last ack: " + fileId);

            if (checkCompleted())
                onDone(true);
        }

        /**
         * @return True if received all request acknowledgements after {@link #markWaitingLastAck()} was called.
         */
        private boolean checkCompleted() {
            for (Map.Entry<UUID, Set<Long>> entry : pendingAcks.entrySet()) {
                Set<Long> reqIds = entry.getValue();

                // If still waiting for some acks.
                if (!reqIds.isEmpty())
                    return false;
            }

            // Got match for each entry in sent map.
            return true;
        }
    }
}
