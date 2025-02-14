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

package org.apache.ignite.internal.processors.cache.persistence.snapshot.dump;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.DynamicCacheDescriptor;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.IgniteCacheOffheapManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.CacheSearchRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractCreateSnapshotFutureTask;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFutureTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotSender;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.tree.CacheDataTree;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersionManager;
import org.apache.ignite.internal.util.BasicRateLimiter;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.cacheDataFilename;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.dumpPartFileName;
import static org.apache.ignite.internal.util.IgniteUtils.toLong;

/**
 * Task creates cache group dump.
 * Dump is a consistent snapshot of cache entries.
 * Directories structure is same as a full snapshot but each partitions saved in "part-0.dump" file.
 * Files structure is a set of {@link DumpEntry} written one by one.
 *
 * @see Dump
 * @see DumpEntry
 */
public class CreateDumpFutureTask extends AbstractCreateSnapshotFutureTask implements DumpEntryChangeListener {
    /** */
    private final FileIOFactory ioFactory;

    /** If {@code true} then compress partition files. */
    private final boolean compress;

    /** Dump transfer rate limiter. */
    private final BasicRateLimiter rateLimiter;

    /** Processed dump size in bytes. */
    private final AtomicLong processedSize = new AtomicLong();

    /** If {@code null} then encryption disabled. */
    private final @Nullable Serializable encKey;

    /**
     * Dump contextes.
     * Key is [group_id, partition_id] combined in single long value.
     *
     * @see IgniteUtils#toLong(int, int)
     */
    private final Map<Long, PartitionDumpContext> dumpCtxs = new ConcurrentHashMap<>();

    /** Local node is primary for set of group partitions. */
    private final Map<Integer, Set<Integer>> grpPrimaries = new ConcurrentHashMap<>();

    /**
     * Map shared across all instances of {@link PartitionDumpContext} and {@link DumpEntrySerializer}.
     * We use per thread buffer because number of threads is fewer then number of partitions.
     * Regular count of partitions is {@link RendezvousAffinityFunction#DFLT_PARTITION_COUNT}
     * and thread is {@link IgniteConfiguration#DFLT_PUBLIC_THREAD_CNT} whic is significantly less.
     */
    private final ConcurrentMap<Long, ByteBuffer> thLocBufs = new ConcurrentHashMap<>();

    /**
     * Map shared across all instances of {@link PartitionDumpContext} and {@link DumpEntrySerializer}.
     * Used to store encrypted data in case dump encrypted.
     */
    private final @Nullable ConcurrentMap<Long, ByteBuffer> encThLocBufs;

    /**
     * Cache entry version at the dump start.
     * Regular updates with {@link IgniteCache#put(Object, Object)} and similar calls use version generated with
     * {@link GridCacheVersionManager#next(GridCacheVersion)}. This version value monotonically grows. It is generated
     * on a <b>primary</b> node and is <b>propagated</b> to the backups. So, on a primary we can distinguish updates
     * that happen before and after the dump start.
     */
    private GridCacheVersion startVer;

    /**
     * @param cctx Cache context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param sft Snapshot file tree.
     * @param ioFactory IO factory.
     * @param snpSndr Snapshot sender.
     * @param rateLimiter Dump transfer rate limiter.
     * @param parts Parts to dump.
     * @param compress If {@code true} then compress partition files.
     * @param encrypt If {@code true} then content of dump encrypted.
     */
    public CreateDumpFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        SnapshotFileTree sft,
        FileIOFactory ioFactory,
        BasicRateLimiter rateLimiter,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts,
        boolean compress,
        boolean encrypt
    ) {
        super(
            cctx,
            srcNodeId,
            reqId,
            sft,
            snpSndr,
            parts
        );

        this.ioFactory = compress ? new WriteOnlyZipFileIOFactory(ioFactory) : new BufferedFileIOFactory(ioFactory);

        this.compress = compress;
        this.rateLimiter = rateLimiter;
        this.encKey = encrypt ? cctx.gridConfig().getEncryptionSpi().create() : null;
        this.encThLocBufs = encrypt ? new ConcurrentHashMap<>() : null;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        try {
            if (log.isInfoEnabled())
                log.info("Start cache dump [name=" + snpName + ", grps=" + parts.keySet() + ']');

            createDumpLock();

            processPartitions();

            prepare();

            saveSnapshotData();
        }
        catch (IgniteCheckedException | IOException e) {
            acceptException(e);
        }

        return false; // Don't wait for checkpoint.
    }

    /** {@inheritDoc} */
    @Override protected void processPartitions() throws IgniteCheckedException {
        super.processPartitions();

        processed.values().forEach(parts -> parts.remove(INDEX_PARTITION));
    }

    /** Prepares all data structures to dump entries. */
    private void prepare() throws IOException, IgniteCheckedException {
        startVer = cctx.versions().next(cctx.kernalContext().discovery().topologyVersion());

        for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
            int grp = e.getKey();

            File grpDumpDir = sft.cacheStorage(cctx.cache().cacheGroup(grp).config());

            if (!grpDumpDir.mkdirs())
                throw new IgniteCheckedException("Dump directory can't be created: " + grpDumpDir);

            CacheGroupContext gctx = cctx.cache().cacheGroup(grp);

            for (GridCacheContext<?, ?> cctx : gctx.caches())
                cctx.dumpListener(this);

            grpPrimaries.put(
                grp,
                gctx.affinity().primaryPartitions(gctx.shared().kernalContext().localNodeId(), gctx.affinity().lastVersion())
            );
        }
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveCacheConfigs() {
        return processed.keySet().stream().map(grp -> runAsync(() -> {
            CacheGroupContext gctx = cctx.cache().cacheGroup(grp);

            File grpDir = sft.cacheStorage(gctx.config());

            IgniteUtils.ensureDirectory(grpDir, "dump group directory", null);

            for (GridCacheContext<?, ?> cacheCtx : gctx.caches()) {
                DynamicCacheDescriptor desc = cctx.kernalContext().cache().cacheDescriptor(cacheCtx.cacheId());

                StoredCacheData cacheData = new StoredCacheData(new CacheConfiguration(desc.cacheConfiguration()));

                cacheData.queryEntities(desc.schema().entities());
                cacheData.sql(desc.sql());

                cctx.cache().configManager().writeCacheData(cacheData, new File(grpDir, cacheDataFilename(cacheData.config())));
            }
        })).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveGroup(int grp, Set<Integer> grpParts) {
        long start = System.currentTimeMillis();

        AtomicLong entriesCnt = new AtomicLong();
        AtomicLong writtenEntriesCnt = new AtomicLong();
        AtomicLong changedEntriesCnt = new AtomicLong();

        String name = cctx.cache().cacheGroup(grp).cacheOrGroupName();

        CacheGroupContext gctx = cctx.kernalContext().cache().cacheGroup(grp);

        if (log.isInfoEnabled())
            log.info("Start group dump [name=" + name + ", id=" + grp + ']');

        List<CompletableFuture<Void>> futs = grpParts.stream().map(part -> runAsync(() -> {
            long entriesCnt0 = 0;
            long writtenEntriesCnt0 = 0;

            int coCtxCache = 0;
            CacheObjectContext coCtx = null;

            try (PartitionDumpContext dumpCtx = dumpContext(grp, part)) {
                try (GridCloseableIterator<CacheDataRow> rows = gctx.offheap().reservedIterator(part, dumpCtx.topVer)) {
                    if (rows == null)
                        throw new IgniteCheckedException("Partition missing [part=" + part + ']');

                    while (rows.hasNext()) {
                        CacheDataRow row = rows.next();

                        assert row.partition() == part;

                        int cache = row.cacheId() == 0 ? grp : row.cacheId();

                        if (cache != coCtxCache) {
                            coCtxCache = cache;

                            coCtx = cctx.cacheObjectContext(coCtxCache);
                        }

                        if (dumpCtx.writeForIterator(cache, row.expireTime(), row.key(), row.value(), row.version(), coCtx))
                            writtenEntriesCnt0++;

                        entriesCnt0++;
                    }
                }
                catch (IOException e) {
                    throw new IgniteException(e);
                }

                entriesCnt.addAndGet(entriesCnt0);
                writtenEntriesCnt.addAndGet(writtenEntriesCnt0);
                changedEntriesCnt.addAndGet(dumpCtx.changedCnt.intValue());

                if (log.isDebugEnabled()) {
                    log.debug("Finish group partition dump [name=" + name +
                        ", id=" + grp +
                        ", part=" + part +
                        ", time=" + (System.currentTimeMillis() - start) +
                        ", iterEntriesCnt=" + entriesCnt +
                        ", writtenIterEntriesCnt=" + entriesCnt +
                        ", changedEntriesCnt=" + changedEntriesCnt + ']');
                }
            }
        })).collect(Collectors.toList());

        int futsSize = futs.size();

        CompletableFuture.allOf(futs.toArray(new CompletableFuture[futsSize])).whenComplete((res, t) -> {
            clearDumpListener(gctx);

            if (log.isInfoEnabled()) {
                log.info("Finish group dump [name=" + name +
                    ", id=" + grp +
                    ", time=" + (System.currentTimeMillis() - start) +
                    ", iterEntriesCnt=" + entriesCnt.get() +
                    ", writtenIterEntriesCnt=" + writtenEntriesCnt.get() +
                    ", changedEntriesCnt=" + changedEntriesCnt.get() + ']');
            }
        });

        return futs;
    }

    /** {@inheritDoc} */
    @Override public void beforeChange(GridCacheContext cctx, KeyCacheObject key, CacheObject val, long expireTime, GridCacheVersion ver) {
        try {
            int part = key.partition();
            int grp = cctx.groupId();

            assert part != -1;

            if (!processed.get(grp).contains(part))
                return;

            dumpContext(grp, part).writeChanged(cctx.cacheId(), expireTime, key, val, ver);
        }
        catch (IgniteException | IgniteCheckedException | IOException e) {
            acceptException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected synchronized CompletableFuture<Void> closeAsync() {
        if (closeFut == null) {
            dumpCtxs.values().forEach(PartitionDumpContext::close);

            Throwable err0 = err.get();

            Set<GroupPartitionId> taken = new HashSet<>();

            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grp = e.getKey();

                clearDumpListener(cctx.cache().cacheGroup(grp));

                for (Integer part : e.getValue())
                    taken.add(new GroupPartitionId(grp, part));
            }

            closeFut = CompletableFuture.runAsync(
                () -> {
                    thLocBufs.clear();
                    if (encThLocBufs != null)
                        encThLocBufs.clear();
                    onDone(new SnapshotFutureTaskResult(taken, null), err0);
                },
                cctx.kernalContext().pools().getSystemExecutorService()
            );
        }

        return closeFut;
    }

    /** @return Processed dump size in bytes. */
    public long processedSize() {
        return processedSize.get();
    }

    /** */
    private void clearDumpListener(CacheGroupContext gctx) {
        for (GridCacheContext<?, ?> cctx : gctx.caches())
            cctx.dumpListener(null);
    }

    /** */
    private void createDumpLock() throws IgniteCheckedException, IOException {
        if (!sft.nodeStorage().mkdirs())
            throw new IgniteCheckedException("Can't create node dump directory: " + sft.nodeStorage().getAbsolutePath());

        if (!sft.dumpLock().createNewFile())
            throw new IgniteCheckedException("Lock file can't be created or already exists: " + sft.dumpLock().getAbsolutePath());
    }

    /** */
    private PartitionDumpContext dumpContext(int grp, int part) {
        return dumpCtxs.computeIfAbsent(
            toLong(grp, part),
            key -> new PartitionDumpContext(cctx.kernalContext().cache().cacheGroup(grp), part)
        );
    }

    /**
     * Context of dump single partition.
     */
    private class PartitionDumpContext implements Closeable {
        /** Group id. */
        private final int grp;

        /** Partition id. */
        private final int part;

        /**
         * Key is cache id, values is set of keys dumped via
         * {@link #writeChanged(int, long, KeyCacheObject, CacheObject, GridCacheVersion)}.
         */
        private final Map<Integer, Set<KeyCacheObject>> changed;

        /** Cache id for {@link #iterLastKey} */
        private int iterLastKeyCache;

        /** Last key dumped via {@link #writeForIterator(int, long, KeyCacheObject, CacheObject, GridCacheVersion, CacheObjectContext)}. */
        private @Nullable KeyCacheObject iterLastKey;

        /** Count of entries changed during dump creation. */
        private final LongAdder changedCnt = new LongAdder();

        /** Partition dump file. Lazily initialized to prevent creation files for empty partitions. */
        private final FileIO file;

        /** Topology Version. */
        private final AffinityTopologyVersion topVer;

        /** Partition serializer. */
        private final DumpEntrySerializer serializer;

        /** If {@code true} context is closed. */
        private volatile boolean closed;

        /**
         * Count of writers. When count becomes {@code 0} context must be closed.
         * By deafult, one writer exists - partition iterator.
         * Each call of {@link #writeChanged(int, long, KeyCacheObject, CacheObject, GridCacheVersion)} increment writers count.
         * When count of writers becomes zero we good to relase all resources associated with partition dump.
         */
        private final AtomicInteger writers = new AtomicInteger(1);

        /** Primary node/partition flag. */
        private final boolean primary;

        /**
         * @param gctx Group context.
         * @param part Partition id.
         */
        private PartitionDumpContext(CacheGroupContext gctx, int part) {
            assert gctx != null;

            try {
                this.part = part;
                grp = gctx.groupId();
                topVer = gctx.topology().lastTopologyChangeVersion();

                primary = grpPrimaries.get(gctx.groupId()).contains(part);

                serializer = new DumpEntrySerializer(
                    thLocBufs,
                    encThLocBufs,
                    encKey,
                    cctx.gridConfig().getEncryptionSpi()
                );
                changed = new HashMap<>();

                for (int cache : gctx.cacheIds())
                    changed.put(cache, new GridConcurrentHashSet<>());

                File dumpFile = new File(sft.cacheStorage(gctx.config()), dumpPartFileName(part, compress));

                if (!dumpFile.createNewFile())
                    throw new IgniteException("Dump file can't be created: " + dumpFile);

                file = ioFactory.create(dumpFile);
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }

        /**
         * Writes entry that changed while dump creation in progress.
         * @param cache Cache id.
         * @param expireTime Expire time.
         * @param key Key.
         * @param val Value before change.
         * @param ver Version before change.
         */
        public void writeChanged(
            int cache,
            long expireTime,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver
        ) throws IgniteCheckedException, IOException {
            String reasonToSkip = null;

            if (closed) // Quick exit. Partition already saved in dump.
                reasonToSkip = "partition already saved";
            else {
                writers.getAndIncrement();

                try {
                    if (closed) // Partition already saved in dump.
                        reasonToSkip = "partition already saved";
                    else if (afterStart(ver))
                        reasonToSkip = "greater version";
                    else {
                        CacheObjectContext coCtx = cctx.cacheObjectContext(cache);

                        synchronized (serializer) { // Prevent concurrent access to the dump file.
                            if (writtenByIterator(cache, key, coCtx))
                                reasonToSkip = "written by iterator"; // Saved by iterator, already. Skip.
                            else if (!changed.get(cache).add(key)) // Entry changed several time during dump.
                                reasonToSkip = "changed several times";
                            else if (val == null)
                                // Previous value is null. Entry created after dump start, skip.
                                reasonToSkip = "newly created or already removed";
                            else
                                write(cache, expireTime, key, val, ver, coCtx);
                        }

                        if (reasonToSkip == null)
                            changedCnt.increment();
                    }
                }
                finally {
                    writers.decrementAndGet();
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Listener [grp=" + grp +
                    ", cache=" + cache +
                    ", part=" + part +
                    ", key=" + key +
                    ", ver=" + ver +
                    ", written=" + (reasonToSkip == null ? "true" : reasonToSkip) +
                    ", startVer=" + (startVer != null) + ']');
            }
        }

        /**
         * Writes entry fetched from partition iterator.
         *
         * @param cache Cache id.
         * @param expireTime Expire time.
         * @param key Key.
         * @param val Value.
         * @param ver Version.
         * @return {@code True} if entry was written in dump.
         * {@code false} if it was already written by {@link #writeChanged(int, long, KeyCacheObject, CacheObject, GridCacheVersion)}.
         */
        public boolean writeForIterator(
            int cache,
            long expireTime,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            CacheObjectContext coCtx
        ) throws IgniteCheckedException, IOException {
            String reason = null;

            if (afterStart(ver))
                reason = "greater version";
            else {
                synchronized (serializer) { // Prevent concurrent access to the dump file.
                    iterLastKeyCache = cache;
                    iterLastKey = key;

                    if (changed.get(cache).contains(key))
                        reason = "written by listener";
                    else
                        write(cache, expireTime, key, val, ver, coCtx);
                }
            }

            if (log.isTraceEnabled()) {
                log.trace("Iterator [" +
                    "grp=" + grp +
                    ", cache=" + cache +
                    ", part=" + part +
                    ", key=" + key +
                    ", written=" + (reason == null ? "true" : reason) +
                    ", ver=" + ver +
                    ", startVer=" + (startVer != null) + ']');
            }

            return reason == null;
        }

        /** */
        private void write(
            int cache,
            long expireTime,
            KeyCacheObject key,
            CacheObject val,
            GridCacheVersion ver,
            CacheObjectContext coCtx
        ) throws IgniteCheckedException, IOException {
            ByteBuffer buf = serializer.writeToBuffer(cache, expireTime, key, val, ver, coCtx);

            rateLimiter.acquire(buf.limit());

            if (file.writeFully(buf) != buf.limit())
                throw new IgniteException("Can't write row");

            processedSize.addAndGet(buf.limit());
        }

        /**
         * Note, usage of {@link IgniteDataStreamer} may lead to dump inconsistency.
         * Because, streamer use the same {@link GridCacheVersion} for all entries.
         * So, we can't efficiently filter out new entries and write them all to dump.
         *
         * @param ver Entry version.
         * @return {@code True} if {@code ver} appeared after dump started.
         * @see GridCacheVersionManager#isolatedStreamerVersion()
         */
        private boolean afterStart(GridCacheVersion ver) {
            return primary && ver.isGreater(startVer);
        }

        /**
         * Iterator returned by {@link IgniteCacheOffheapManager#reservedIterator(int, AffinityTopologyVersion)}
         * iterates key in ascending order set by {@link CacheDataTree#compare(BPlusIO, long, int, CacheSearchRow)}.
         * So if key changed by the user (see {@link #writeChanged(int, long, KeyCacheObject, CacheObject, GridCacheVersion)})
         * is greater than last key written by partition iterator then it hasn't been saved in dump yet and must be written.
         * Otherwise, key already saved by the iterator and must be skiped.
         *
         * @param cache Cache id.
         * @param key Key to write with {@link #writeChanged(int, long, KeyCacheObject, CacheObject, GridCacheVersion)}.
         * @return {@code True} if key written by iterator, already. {@code False} otherwise.
         * @see CacheDataTree#compareBytes(byte[], byte[])
         */
        private boolean writtenByIterator(int cache, KeyCacheObject key, CacheObjectContext coCtx) throws IgniteCheckedException {
            if (iterLastKey == null)
                return false;

            int cmp = Integer.compare(iterLastKeyCache, cache);

            if (cmp != 0)
                return cmp > 0;

            cmp = Integer.compare(iterLastKey.hashCode(), key.hashCode());

            if (cmp != 0)
                return cmp > 0;

            byte[] bytes1 = iterLastKey.valueBytes(coCtx);
            byte[] bytes2 = key.valueBytes(coCtx);

            return CacheDataTree.compareBytes(bytes1, bytes2) >= 0;
        }

        /** {@inheritDoc} */
        @Override public void close() {
            synchronized (this) { // Prevent concurrent close invocation.
                if (closed)
                    return;

                closed = true;
            }

            writers.decrementAndGet();

            while (writers.get() > 0) // Waiting for all on the fly listeners to complete.
                LockSupport.parkNanos(1_000_000);

            try {
                file.close();
            }
            catch (IOException e) {
                throw new IgniteException(e);
            }
        }
    }

    /** @return Encryption key. */
    public @Nullable Serializable encryptionKey() {
        return encKey;
    }
}
