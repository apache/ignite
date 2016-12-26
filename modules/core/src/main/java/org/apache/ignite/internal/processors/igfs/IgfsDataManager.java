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

import java.util.concurrent.Executor;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteInterruptedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.AffinityKeyMapper;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.igfs.IgfsBlockLocation;
import org.apache.ignite.igfs.IgfsException;
import org.apache.ignite.igfs.IgfsGroupDataBlocksKeyMapper;
import org.apache.ignite.igfs.IgfsOutOfSpaceException;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.secondary.IgfsSecondaryFileSystemPositionedReadable;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.IgniteInternalCache;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.datastreamer.DataStreamerCacheUpdaters;
import org.apache.ignite.internal.processors.igfs.data.IgfsDataPutProcessor;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.CX1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import java.io.DataInput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_IGFS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.IGFS_POOL;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 * Cache based file's data container.
 */
public class IgfsDataManager extends IgfsManager {
    /** Data internal cache. */
    private IgniteInternalCache<IgfsBlockKey, byte[]> dataCachePrj;

    /** Data cache. */
    private IgniteInternalCache<Object, Object> dataCache;

    /** */
    private CountDownLatch dataCacheStartLatch;

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

    /** Request ID counter for write messages. */
    private AtomicLong reqIdCtr = new AtomicLong();

    /** IGFS communication topic. */
    private Object topic;

    /** Async file delete worker. */
    private AsyncDeleteWorker delWorker;

    /** On-going remote reads futures. */
    private final ConcurrentHashMap8<IgfsBlockKey, IgniteInternalFuture<byte[]>> rmtReadFuts =
        new ConcurrentHashMap8<>();

    /**
     *
     */
    void awaitInit() {
        try {
            dataCacheStartLatch.await();
        }
        catch (InterruptedException e) {
            throw new IgniteInterruptedException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        dataCacheStartLatch = new CountDownLatch(1);

        String igfsName = igfsCtx.configuration().getName();

        topic = F.isEmpty(igfsName) ? TOPIC_IGFS : TOPIC_IGFS.topic(igfsName);

        igfsCtx.kernalContext().io().addMessageListener(topic, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg) {
                if (msg instanceof IgfsBlocksMessage)
                    processBlocksMessage(nodeId, (IgfsBlocksMessage)msg);
                else if (msg instanceof IgfsAckMessage)
                    processAckMessage(nodeId, (IgfsAckMessage)msg);
            }
        });

        igfsCtx.kernalContext().event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt.type() == EVT_NODE_FAILED || evt.type() == EVT_NODE_LEFT;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                if (igfsCtx.igfsNode(discoEvt.eventNode())) {
                    for (WriteCompletionFuture future : pendingWrites.values()) {
                        future.onError(discoEvt.eventNode().id(),
                            new ClusterTopologyCheckedException("Node left grid before write completed: " + evt.node().id()));
                    }
                }
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        delWorker = new AsyncDeleteWorker(igfsCtx.kernalContext().gridName(),
            "igfs-" + igfsName + "-delete-worker", log);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override protected void onKernalStart0() throws IgniteCheckedException {
        dataCachePrj = igfsCtx.kernalContext().cache().getOrStartCache(igfsCtx.configuration().getDataCacheName());

        assert dataCachePrj != null;

        dataCache = (IgniteInternalCache)dataCachePrj;

        AffinityKeyMapper mapper = igfsCtx.kernalContext().cache()
            .internalCache(igfsCtx.configuration().getDataCacheName()).configuration().getAffinityMapper();

        grpSize = mapper instanceof IgfsGroupDataBlocksKeyMapper ?
            ((IgfsGroupDataBlocksKeyMapper)mapper).getGroupSize() : 1;

        grpBlockSize = igfsCtx.configuration().getBlockSize() * grpSize;

        assert grpBlockSize != 0;

        igfsCtx.kernalContext().cache().internalCache(igfsCtx.configuration().getDataCacheName()).preloader()
            .startFuture().listen(new CI1<IgniteInternalFuture<Object>>() {
            @Override public void apply(IgniteInternalFuture<Object> f) {
                dataCacheStartLatch.countDown();
            }
        });

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
    }

    /**
     * @return Number of bytes used to store files.
     */
    public long spaceSize() {
        return dataCachePrj.igfsDataSpaceUsed();
    }

    /**
     * @return Maximum number of bytes for IGFS data cache.
     */
    public long maxSpaceSize() {
        return (igfsCtx.configuration().getMaxSpaceSize() <= 0) ? 0 : dataCachePrj.igfsDataSpaceMax();
    }

    /**
     * Generates next affinity key for local node based on current topology. If previous affinity key maps
     * on local node, return previous affinity key to prevent unnecessary file map growth.
     *
     * @param prevAffKey Affinity key of previous block.
     * @return Affinity key.
     */
    @SuppressWarnings("ConstantConditions")
    public IgniteUuid nextAffinityKey(@Nullable IgniteUuid prevAffKey) {
        // Do not generate affinity key for non-affinity nodes.
        if (!dataCache.context().affinityNode())
            return null;

        UUID nodeId = igfsCtx.kernalContext().localNodeId();

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
     * Creates new instance of explicit data streamer.
     *
     * @return New instance of data streamer.
     */
    private IgniteDataStreamer<IgfsBlockKey, byte[]> dataStreamer() {
        IgniteDataStreamer<IgfsBlockKey, byte[]> ldr =
            igfsCtx.kernalContext().<IgfsBlockKey, byte[]>dataStream().dataStreamer(dataCachePrj.name());

        FileSystemConfiguration cfg = igfsCtx.configuration();

        if (cfg.getPerNodeBatchSize() > 0)
            ldr.perNodeBufferSize(cfg.getPerNodeBatchSize());

        if (cfg.getPerNodeParallelBatchCount() > 0)
            ldr.perNodeParallelOperations(cfg.getPerNodeParallelBatchCount());

        ldr.receiver(DataStreamerCacheUpdaters.<IgfsBlockKey, byte[]>batchedSorted());

        return ldr;
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
    @Nullable public IgniteInternalFuture<byte[]> dataBlock(final IgfsEntryInfo fileInfo, final IgfsPath path,
        final long blockIdx, @Nullable final IgfsSecondaryFileSystemPositionedReadable secReader)
        throws IgniteCheckedException {
        assert fileInfo != null;
        assert blockIdx >= 0;

        // Schedule block request BEFORE prefetch requests.
        final IgfsBlockKey key = blockKey(blockIdx, fileInfo);

        if (log.isDebugEnabled() &&
            dataCache.affinity().isPrimaryOrBackup(igfsCtx.kernalContext().discovery().localNode(), key)) {
            log.debug("Reading non-local data block [path=" + path + ", fileInfo=" + fileInfo +
                ", blockIdx=" + blockIdx + ']');
        }

        IgniteInternalFuture<byte[]> fut = dataCachePrj.getAsync(key);

        if (secReader != null) {
            Executor exec = igfsCtx.kernalContext().pools().poolForPolicy(GridIoPolicy.IGFS_POOL);

            fut = fut.chain(new CX1<IgniteInternalFuture<byte[]>, byte[]>() {
                @Override public byte[] applyx(IgniteInternalFuture<byte[]> fut) throws IgniteCheckedException {
                    byte[] res = fut.get();

                    if (res == null) {
                        GridFutureAdapter<byte[]> rmtReadFut = new GridFutureAdapter<>();

                        IgniteInternalFuture<byte[]> oldRmtReadFut = rmtReadFuts.putIfAbsent(key, rmtReadFut);

                        if (oldRmtReadFut == null) {
                            try {
                                res = secondaryDataBlock(path, blockIdx, secReader, fileInfo.blockSize());

                                rmtReadFut.onDone(res);

                                putBlock(fileInfo.blockSize(), key, res);
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

                            igfsCtx.metrics().addReadBlocks(1, 0);
                        }
                    }
                    else
                        igfsCtx.metrics().addReadBlocks(1, 0);

                    return res;
                }
            }, exec);
        }
        else
            igfsCtx.metrics().addReadBlocks(1, 0);

        return fut;
    }

    /**
     * Get data block for specified block index from secondary reader.
     *
     * @param path Path reading from.
     * @param blockIdx Block index.
     * @param secReader Optional secondary file system reader.
     * @param blockSize Block size.
     * @return Requested data block or {@code null} if nothing found.
     * @throws IgniteCheckedException If failed.
     */
    @Nullable public byte[] secondaryDataBlock(IgfsPath path, long blockIdx,
        IgfsSecondaryFileSystemPositionedReadable secReader, int blockSize) throws IgniteCheckedException {
        if (log.isDebugEnabled())
            log.debug("Reading non-local data block in the secondary file system [path=" +
                path + ", blockIdx=" + blockIdx + ']');

        long pos = blockIdx * blockSize; // Calculate position for Hadoop

        byte[] res = new byte[blockSize];

        int read = 0;

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

        // If we did not read full block at the end of the file - trim it.
        if (read != blockSize)
            res = Arrays.copyOf(res, read);

        igfsCtx.metrics().addReadBlocks(1, 1);

        return res;
    }

    /**
     * Stores the given block in data cache.
     *
     * @param blockSize The size of the block.
     * @param key The data cache key of the block.
     * @param data The new value of the block.
     * @throws IgniteCheckedException If failed.
     */
    private void putBlock(int blockSize, IgfsBlockKey key, byte[] data) throws IgniteCheckedException {
        if (data.length < blockSize)
            // partial (incomplete) block:
            dataCachePrj.invoke(key, new IgfsDataPutProcessor(data));
        else {
            // whole block:
            assert data.length == blockSize;

            dataCachePrj.put(key, data);
        }
    }


    /**
     * Registers write future in igfs data manager.
     *
     * @param fileId File ID.
     * @return Future that will be completed when all ack messages are received or when write failed.
     */
    public IgniteInternalFuture<Boolean> writeStart(IgniteUuid fileId) {
        WriteCompletionFuture fut = new WriteCompletionFuture(fileId);

        WriteCompletionFuture oldFut = pendingWrites.putIfAbsent(fileId, fut);

        assert oldFut == null : "Opened write that is being concurrently written: " + fileId;

        if (log.isDebugEnabled())
            log.debug("Registered write completion future for file output stream [fileId=" + fileId +
                ", fut=" + fut + ']');

        return fut;
    }

    /**
     * Notifies data manager that no further writes will be performed on stream.
     *
     * @param fileId File ID.
     * @throws IgniteCheckedException If failed.
     */
    public void writeClose(IgniteUuid fileId) throws IgniteCheckedException {
        WriteCompletionFuture fut = pendingWrites.get(fileId);

        if (fut != null)
            fut.markWaitingLastAck();
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
        IgfsEntryInfo fileInfo,
        long reservedLen,
        @Nullable byte[] remainder,
        int remainderLen,
        ByteBuffer data,
        boolean flush,
        IgfsFileAffinityRange affinityRange,
        @Nullable IgfsFileWorkerBatch batch
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
        IgfsEntryInfo fileInfo,
        long reservedLen,
        @Nullable byte[] remainder,
        int remainderLen,
        DataInput in,
        int len,
        boolean flush,
        IgfsFileAffinityRange affinityRange,
        @Nullable IgfsFileWorkerBatch batch
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
    public IgniteInternalFuture<Object> delete(IgfsEntryInfo fileInfo) {
        if (!fileInfo.isFile()) {
            if (log.isDebugEnabled())
                log.debug("Cannot delete content of not-data file: " + fileInfo);

            return new GridFinishedFuture<>();
        }
        else
            return delWorker.deleteAsync(fileInfo);
    }

    /**
     * @param blockIdx Block index.
     * @param fileInfo File info.
     * @return Block key.
     */
    public IgfsBlockKey blockKey(long blockIdx, IgfsEntryInfo fileInfo) {
        if (fileInfo.affinityKey() != null)
            return new IgfsBlockKey(fileInfo.id(), fileInfo.affinityKey(), fileInfo.evictExclude(), blockIdx);

        if (fileInfo.fileMap() != null) {
            IgniteUuid affKey = fileInfo.fileMap().affinityKey(blockIdx * fileInfo.blockSize(), false);

            return new IgfsBlockKey(fileInfo.id(), affKey, fileInfo.evictExclude(), blockIdx);
        }

        return new IgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), blockIdx);
    }

    /**
     * Tries to remove blocks affected by fragmentizer. If {@code cleanNonColocated} is {@code true}, will remove
     * non-colocated blocks as well.
     *
     * @param fileInfo File info to clean up.
     * @param range Range to clean up.
     * @param cleanNonColocated {@code True} if all blocks should be cleaned.
     */
    public void cleanBlocks(IgfsEntryInfo fileInfo, IgfsFileAffinityRange range, boolean cleanNonColocated) {
        long startIdx = range.startOffset() / fileInfo.blockSize();

        long endIdx = range.endOffset() / fileInfo.blockSize();

        if (log.isDebugEnabled())
            log.debug("Cleaning blocks [fileInfo=" + fileInfo + ", range=" + range +
                ", cleanNonColocated=" + cleanNonColocated + ", startIdx=" + startIdx + ", endIdx=" + endIdx + ']');

        try {
            try (IgniteDataStreamer<IgfsBlockKey, byte[]> ldr = dataStreamer()) {
                for (long idx = startIdx; idx <= endIdx; idx++) {
                    ldr.removeData(new IgfsBlockKey(fileInfo.id(), range.affinityKey(), fileInfo.evictExclude(),
                        idx));

                    if (cleanNonColocated)
                        ldr.removeData(new IgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), idx));
                }
            }
        }
        catch (IgniteException e) {
            log.error("Failed to clean up file range [fileInfo=" + fileInfo + ", range=" + range + ']', e);
        }
    }

    /**
     * Moves all collocated blocks in range to non-colocated keys.
     * @param fileInfo File info to move data for.
     * @param range Range to move.
     */
    public void spreadBlocks(IgfsEntryInfo fileInfo, IgfsFileAffinityRange range) {
        long startIdx = range.startOffset() / fileInfo.blockSize();

        long endIdx = range.endOffset() / fileInfo.blockSize();

        try {
            try (IgniteDataStreamer<IgfsBlockKey, byte[]> ldr = dataStreamer()) {
                long bytesProcessed = 0;

                for (long idx = startIdx; idx <= endIdx; idx++) {
                    IgfsBlockKey colocatedKey = new IgfsBlockKey(fileInfo.id(), range.affinityKey(),
                        fileInfo.evictExclude(), idx);

                    IgfsBlockKey key = new IgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), idx);

                    // Most of the time should be local get.
                    byte[] block = dataCachePrj.get(colocatedKey);

                    if (block != null) {
                        // Need to check if block is partially written.
                        // If so, must update it in pessimistic transaction.
                        if (block.length != fileInfo.blockSize()) {
                            try (IgniteInternalTx tx = dataCachePrj.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
                                Map<IgfsBlockKey, byte[]> vals = dataCachePrj.getAll(F.asList(colocatedKey, key));

                                byte[] val = vals.get(colocatedKey);

                                if (val != null) {
                                    putBlock(fileInfo.blockSize(), key, val);

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

                        if (bytesProcessed >= igfsCtx.configuration().getFragmentizerThrottlingBlockLength()) {
                            ldr.flush();

                            bytesProcessed = 0;

                            U.sleep(igfsCtx.configuration().getFragmentizerThrottlingDelay());
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
    public Collection<IgfsBlockLocation> affinity(IgfsEntryInfo info, long start, long len)
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
    public Collection<IgfsBlockLocation> affinity(IgfsEntryInfo info, long start, long len, long maxLen)
        throws IgniteCheckedException {
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
            Collection<IgfsBlockLocation> res = new LinkedList<>();

            splitBlocks(start, len, maxLen, dataCache.affinity().mapKeyToPrimaryAndBackups(
                new IgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(), 0)), res);

            return res;
        }

        // Need to merge ranges affinity with non-colocated affinity.
        Deque<IgfsBlockLocation> res = new LinkedList<>();

        if (info.fileMap().ranges().isEmpty()) {
            affinity0(info, start, len, maxLen, res);

            return res;
        }

        long pos = start;
        long end = start + len;

        for (IgfsFileAffinityRange range : info.fileMap().ranges()) {
            if (log.isDebugEnabled())
                log.debug("Checking range [range=" + range + ", pos=" + pos + ']');

            // If current position is less than range start, add non-colocated affinity ranges.
            if (range.less(pos)) {
                long partEnd = Math.min(end, range.startOffset());

                affinity0(info, pos, partEnd - pos, maxLen, res);

                pos = partEnd;
            }

            IgfsBlockLocation last = res.peekLast();

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
     */
    private void affinity0(IgfsEntryInfo info, long start, long len, long maxLen, Deque<IgfsBlockLocation> res) {
        long firstGrpIdx = start / grpBlockSize;
        long limitGrpIdx = (start + len + grpBlockSize - 1) / grpBlockSize;

        if (limitGrpIdx - firstGrpIdx > Integer.MAX_VALUE)
            throw new IgfsException("Failed to get affinity (range is too wide)" +
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
            IgfsBlockKey key = new IgfsBlockKey(info.id(), info.affinityKey(), info.evictExclude(),
                grpIdx * grpSize);

            Collection<ClusterNode> affNodes = dataCache.affinity().mapKeyToPrimaryAndBackups(key);

            if (log.isDebugEnabled())
                log.debug("Mapped key to nodes [key=" + key + ", nodes=" + F.nodeIds(affNodes) +
                ", blockStart=" + blockStart + ", blockLen=" + blockLen + ']');

            IgfsBlockLocation last = res.peekLast();

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
        Collection<ClusterNode> nodes, Collection<IgfsBlockLocation> res) {
        if (maxLen > 0) {
            long end = start + len;

            long start0 = start;

            while (start0 < end) {
                long len0 = Math.min(maxLen, end - start0);

                res.add(new IgfsBlockLocationImpl(start0, len0, nodes));

                start0 += len0;
            }
        }
        else
            res.add(new IgfsBlockLocationImpl(start, len, nodes));
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
     * @param fileId File ID.
     * @param node Node to process blocks on.
     * @param blocks Blocks to put in cache.
     * @throws IgniteCheckedException If batch processing failed.
     */
    private void processBatch(IgniteUuid fileId, final ClusterNode node,
        final Map<IgfsBlockKey, byte[]> blocks) throws IgniteCheckedException {
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
            final IgfsBlocksMessage msg = new IgfsBlocksMessage(fileId, batchId, blocks);

            try {
                igfsCtx.send(nodeId, topic, msg, IGFS_POOL);
            }
            catch (IgniteCheckedException e) {
                completionFut.onError(nodeId, e);
            }
        }
        else {
            igfsCtx.runInIgfsThreadPool(new Runnable() {
                @Override public void run() {
                    storeBlocksAsync(blocks).listen(new CI1<IgniteInternalFuture<?>>() {
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
     * @param blockSize The block size.
     * @throws IgniteCheckedException If update failed.
     */
    private void processPartialBlockWrite(IgniteUuid fileId, IgfsBlockKey colocatedKey, int startOff,
        byte[] data, int blockSize) throws IgniteCheckedException {
        if (dataCachePrj.igfsDataSpaceUsed() >= dataCachePrj.igfsDataSpaceMax()) {
            final WriteCompletionFuture completionFut = pendingWrites.get(fileId);

            if (completionFut == null) {
                if (log.isDebugEnabled())
                    log.debug("Missing completion future for file write request (most likely exception occurred " +
                        "which will be thrown upon stream close) [fileId=" + fileId + ']');

                return;
            }

            IgfsOutOfSpaceException e = new IgfsOutOfSpaceException("Failed to write data block " +
                "(IGFS maximum data size exceeded) [used=" + dataCachePrj.igfsDataSpaceUsed() +
                ", allowed=" + dataCachePrj.igfsDataSpaceMax() + ']');

            completionFut.onDone(new IgniteCheckedException("Failed to write data (not enough space on node): " +
                igfsCtx.kernalContext().localNodeId(), e));

            return;
        }

        // No affinity key present, just concat and return.
        if (colocatedKey.affinityKey() == null) {
            dataCachePrj.invoke(colocatedKey, new UpdateProcessor(startOff, data));

            return;
        }

        // If writing from block beginning, just put and return.
        if (startOff == 0) {
            putBlock(blockSize, colocatedKey, data);

            return;
        }

        // Create non-colocated key.
        IgfsBlockKey key = new IgfsBlockKey(colocatedKey.getFileId(), null,
            colocatedKey.evictExclude(), colocatedKey.getBlockId());

        try (IgniteInternalTx tx = dataCachePrj.txStartEx(PESSIMISTIC, REPEATABLE_READ)) {
            // Lock keys.
            Map<IgfsBlockKey, byte[]> vals = dataCachePrj.getAll(F.asList(colocatedKey, key));

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
     * @param blocks Blocks to write.
     * @return Future that will be completed after put is done.
     */
    @SuppressWarnings("unchecked")
    private IgniteInternalFuture<?> storeBlocksAsync(Map<IgfsBlockKey, byte[]> blocks) {
        assert !blocks.isEmpty();

        if (dataCachePrj.igfsDataSpaceUsed() >= dataCachePrj.igfsDataSpaceMax()) {
            return new GridFinishedFuture<Object>(
                new IgfsOutOfSpaceException("Failed to write data block (IGFS maximum data size " +
                    "exceeded) [used=" + dataCachePrj.igfsDataSpaceUsed() +
                    ", allowed=" + dataCachePrj.igfsDataSpaceMax() + ']'));
        }

        return dataCachePrj.putAllAsync(blocks);
    }

    /**
     * @param nodeId Node ID.
     * @param blocksMsg Write request message.
     */
    private void processBlocksMessage(final UUID nodeId, final IgfsBlocksMessage blocksMsg) {
        storeBlocksAsync(blocksMsg.blocks()).listen(new CI1<IgniteInternalFuture<?>>() {
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
                    igfsCtx.send(nodeId, topic, new IgfsAckMessage(blocksMsg.fileId(), blocksMsg.id(), err), IGFS_POOL);
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
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void processAckMessage(UUID nodeId, IgfsAckMessage ackMsg) {
        try {
            ackMsg.finishUnmarshal(igfsCtx.kernalContext().config().getMarshaller(), null);
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
    private IgfsBlockKey createBlockKey(
        long block,
        IgfsEntryInfo fileInfo,
        IgfsFileAffinityRange locRange
    ) {
        // If affinityKey is present, return block key as is.
        if (fileInfo.affinityKey() != null)
            return new IgfsBlockKey(fileInfo.id(), fileInfo.affinityKey(), fileInfo.evictExclude(), block);

        // If range is done, no colocated writes are attempted.
        if (locRange == null)
            return new IgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(), block);

        long blockStart = block * fileInfo.blockSize();

        // If block does not belong to new range, return old affinity key.
        if (locRange.less(blockStart)) {
            IgniteUuid affKey = fileInfo.fileMap().affinityKey(blockStart, false);

            return new IgfsBlockKey(fileInfo.id(), affKey, fileInfo.evictExclude(), block);
        }

        if (!locRange.belongs(blockStart))
            locRange.expand(blockStart, fileInfo.blockSize());

        return new IgfsBlockKey(fileInfo.id(), locRange.affinityKey(), fileInfo.evictExclude(), block);
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
        @SuppressWarnings("ConstantConditions")
        @Nullable public byte[] storeDataBlocks(
            IgfsEntryInfo fileInfo,
            long reservedLen,
            @Nullable byte[] remainder,
            final int remainderLen,
            T src,
            int srcLen,
            boolean flush,
            IgfsFileAffinityRange affinityRange,
            @Nullable IgfsFileWorkerBatch batch
        ) throws IgniteCheckedException {
            IgniteUuid id = fileInfo.id();
            int blockSize = fileInfo.blockSize();

            int len = remainderLen + srcLen;

            if (len > reservedLen)
                throw new IgfsException("Not enough space reserved to store data [id=" + id +
                    ", reservedLen=" + reservedLen + ", remainderLen=" + remainderLen +
                    ", data.length=" + srcLen + ']');

            long start = reservedLen - len;
            long first = start / blockSize;
            long limit = (start + len + blockSize - 1) / blockSize;
            int written = 0;
            int remainderOff = 0;

            Map<IgfsBlockKey, byte[]> nodeBlocks = U.newLinkedHashMap((int)(limit - first));
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
                IgfsBlockKey key = createBlockKey(block, fileInfo, affinityRange);

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

                            igfsCtx.metrics().addWriteBlocks(1, 0);
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
                    processPartialBlockWrite(id, key, block == first ? off : 0, portion, blockSize);

                    writtenTotal++;
                }
                else
                    nodeBlocks.put(key, portion);

                igfsCtx.metrics().addWriteBlocks(writtenTotal, writtenSecondary);

                written += portion.length;
            }

            // Process final batch, if exists.
            if (!nodeBlocks.isEmpty()) {
                processBatch(id, node, nodeBlocks);

                igfsCtx.metrics().addWriteBlocks(nodeBlocks.size(), 0);
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
    private static final class UpdateProcessor implements EntryProcessor<IgfsBlockKey, byte[], Void>,
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
        @Override public Void process(MutableEntry<IgfsBlockKey, byte[]> entry, Object... args) {
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
        private final IgfsEntryInfo stopInfo;

        /** Delete requests queue. */
        private BlockingQueue<IgniteBiTuple<GridFutureAdapter<Object>, IgfsEntryInfo>> delReqs =
            new LinkedBlockingQueue<>();

        /**
         * @param gridName Grid name.
         * @param name Worker name.
         * @param log Log.
         */
        protected AsyncDeleteWorker(@Nullable String gridName, String name, IgniteLogger log) {
            super(gridName, name, log);

            stopInfo = IgfsUtils.createDirectory(IgniteUuid.randomUuid());
        }

        /**
         * Gracefully stops worker by adding STOP_INFO to queue.
         */
        private void stop() {
            delReqs.offer(F.t(new GridFutureAdapter<>(), stopInfo));
        }

        /**
         * @param info File info to delete.
         * @return Future which completes when entry is actually removed.
         */
        private IgniteInternalFuture<Object> deleteAsync(IgfsEntryInfo info) {
            GridFutureAdapter<Object> fut = new GridFutureAdapter<>();

            delReqs.offer(F.t(fut, info));

            return fut;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ConstantConditions")
        @Override protected void body() throws InterruptedException, IgniteInterruptedCheckedException {
            try {
                while (!isCancelled()) {
                    IgniteBiTuple<GridFutureAdapter<Object>, IgfsEntryInfo> req = delReqs.take();

                    GridFutureAdapter<Object> fut = req.get1();
                    IgfsEntryInfo fileInfo = req.get2();

                    // Identity check.
                    if (fileInfo == stopInfo) {
                        fut.onDone();

                        break;
                    }

                    IgniteDataStreamer<IgfsBlockKey, byte[]> ldr = dataStreamer();

                    try {
                        IgfsFileMap map = fileInfo.fileMap();

                        for (long block = 0, size = fileInfo.blocksCount(); block < size; block++) {
                            IgniteUuid affKey = map == null ? null : map.affinityKey(block * fileInfo.blockSize(), true);

                            ldr.removeData(new IgfsBlockKey(fileInfo.id(), affKey, fileInfo.evictExclude(),
                                block));

                            if (affKey != null)
                                ldr.removeData(new IgfsBlockKey(fileInfo.id(), null, fileInfo.evictExclude(),
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
                                ldr.removeData(new IgfsBlockKey(fileId, fileInfo.affinityKey(),
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
                                log.error("Failed to stop data streamer while shutting down " +
                                    "igfs async delete thread.", e);
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
                    log.debug("Stopping asynchronous igfs file delete thread: " + name());

                IgniteBiTuple<GridFutureAdapter<Object>, IgfsEntryInfo> req = delReqs.poll();

                while (req != null) {
                    req.get1().onCancelled();

                    req = delReqs.poll();
                }
            }
        }
    }

    /**
     * Allows output stream to await for all current acks.
     *
     * @param fileId File ID.
     * @throws IgniteInterruptedCheckedException In case of interrupt.
     */
    void awaitAllAcksReceived(IgniteUuid fileId) throws IgniteInterruptedCheckedException {
        WriteCompletionFuture fut = pendingWrites.get(fileId);

        if (fut != null)
            fut.awaitAllAcksReceived();
    }

    /**
     * Future that is completed when all participating
     */
    private class WriteCompletionFuture extends GridFutureAdapter<Boolean> {
        /** */
        private static final long serialVersionUID = 0L;

        /** File id to remove future from map. */
        private final IgniteUuid fileId;

        /** Pending acks. */
        private final ConcurrentMap<Long, UUID> ackMap = new ConcurrentHashMap8<>();

        /** Lock for map-related conditions. */
        private final Lock lock = new ReentrantLock();

        /** Condition to wait for empty map. */
        private final Condition allAcksRcvCond = lock.newCondition();

        /** Flag indicating future is waiting for last ack. */
        private volatile boolean awaitingLast;

        /**
         * @param fileId File id.
         */
        private WriteCompletionFuture(IgniteUuid fileId) {
            assert fileId != null;

            this.fileId = fileId;
        }

        /**
         * Await all pending data blockes to be acked.
         *
         * @throws IgniteInterruptedCheckedException In case of interrupt.
         */
        public void awaitAllAcksReceived() throws IgniteInterruptedCheckedException {
            lock.lock();

            try {
                while (!ackMap.isEmpty())
                    U.await(allAcksRcvCond);
            }
            finally {
                lock.unlock();
            }
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
                UUID pushedOut = ackMap.putIfAbsent(batchId, nodeId);

                assert pushedOut == null;
            }
        }

        /**
         * Answers if there are some batches for the specified node we're currently waiting acks for.
         *
         * @param nodeId The node Id.
         * @return If there are acks awaited from this node.
         */
        private boolean hasPendingAcks(UUID nodeId) {
            assert nodeId != null;

            for (Map.Entry<Long, UUID> e : ackMap.entrySet())
                if (nodeId.equals(e.getValue()))
                    return true;

            return false;
        }

        /**
         * Error occurred on node with given ID.
         *
         * @param nodeId Node ID.
         * @param e Caught exception.
         */
        private void onError(UUID nodeId, IgniteCheckedException e) {
            // If waiting for ack from this node.
            if (hasPendingAcks(nodeId)) {
                ackMap.clear();

                signalNoAcks();

                if (e.hasCause(IgfsOutOfSpaceException.class))
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
                boolean rmv = ackMap.remove(batchId, nodeId);

                assert rmv : "Received acknowledgement message for not registered batch [nodeId=" +
                    nodeId + ", batchId=" + batchId + ']';

                if (ackMap.isEmpty()) {
                    signalNoAcks();

                    if (awaitingLast)
                        onDone(true);
                }
            }
        }

        /**
         * Signal that currenlty there are no more pending acks.
         */
        private void signalNoAcks() {
            lock.lock();

            try {
                allAcksRcvCond.signalAll();
            }
            finally {
                lock.unlock();
            }
        }

        /**
         * Marks this future as waiting last ack.
         */
        private void markWaitingLastAck() {
            awaitingLast = true;

            if (log.isDebugEnabled())
                log.debug("Marked write completion future as awaiting last ack: " + fileId);

            if (ackMap.isEmpty())
                onDone(true);
        }
    }
}