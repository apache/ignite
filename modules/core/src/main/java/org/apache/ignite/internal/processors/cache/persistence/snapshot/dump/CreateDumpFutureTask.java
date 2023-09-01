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
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractCreateSnapshotFutureTask;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFutureTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotSender;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.cachDataFilename;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.PART_FILE_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;

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
    /** Dump files name. */
    public static final String DUMP_FILE_EXT = ".dump";

    /** Root dump directory. */
    private final File dumpDir;

    /** */
    private final FileIOFactory ioFactory;

    /**
     * Dump contextes.
     * Key is [group_id, partition_id] combined in single long value.
     *
     * @see #toLong(int, int)
     */
    private final Map<Long, PartitionDumpContext> dumpCtxs = new ConcurrentHashMap<>();

    /**
     * @param cctx Cache context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param dumpName Dump name.
     * @param ioFactory IO factory.
     * @param snpSndr Snapshot sender.
     * @param parts Parts to dump.
     */
    public CreateDumpFutureTask(
        GridCacheSharedContext<?, ?> cctx,
        UUID srcNodeId,
        UUID reqId,
        String dumpName,
        File dumpDir,
        FileIOFactory ioFactory,
        SnapshotSender snpSndr,
        Map<Integer, Set<Integer>> parts
    ) {
        super(
            cctx,
            srcNodeId,
            reqId,
            dumpName,
            snpSndr,
            parts
        );

        this.dumpDir = dumpDir;
        this.ioFactory = ioFactory;
    }

    /** {@inheritDoc} */
    @Override public boolean start() {
        try {
            log.info("Start cache dump [name=" + snpName + ", grps=" + parts.keySet() + ']');

            createDumpLock();

            processPartitions();

            prepare();

            startAllAsync();
        }
        catch (IgniteCheckedException | IOException e) {
            acceptException(e);

            onDone(e);
        }

        return false; // Don't wait for checkpoint.
    }

    /** Prepares all data structures to dump entries. */
    private void prepare() throws IOException, IgniteCheckedException {
        for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
            int grp = e.getKey();

            File grpDumpDir = groupDirectory(cctx.cache().cacheGroup(grp));

            if (!grpDumpDir.mkdirs())
                throw new IgniteCheckedException("Dump directory can't be created: " + grpDumpDir);

            for (int part : e.getValue()) {
                PartitionDumpContext prev = dumpCtxs.put(
                    toLong(grp, part),
                    new PartitionDumpContext(grp, part, new File(grpDumpDir, PART_FILE_PREFIX + part + DUMP_FILE_EXT))
                );

                assert prev == null;
            }

            CacheGroupContext gctx = cctx.cache().cacheGroup(grp);

            for (GridCacheContext<?, ?> cctx : gctx.caches())
                cctx.dumpListener(this);
        }
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveCacheConfigsCopy() {
        return parts.keySet().stream().map(grp -> future(() -> {
            CacheGroupContext gctx = cctx.cache().cacheGroup(grp);

            File grpDir = groupDirectory(gctx);

            IgniteUtils.ensureDirectory(grpDir, "dump group directory", null);

            for (GridCacheContext<?, ?> cacheCtx : gctx.caches()) {
                CacheConfiguration<?, ?> ccfg = cacheCtx.config();

                cctx.cache().configManager().writeCacheData(
                    new StoredCacheData(ccfg),
                    new File(grpDir, cachDataFilename(ccfg))
                );
            }
        })).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveGroup(int grp, Set<Integer> grpParts) {
        long start = System.currentTimeMillis();

        AtomicLong entriesCnt = new AtomicLong();
        AtomicLong changedEntriesCnt = new AtomicLong();
        AtomicLong partsRemain = new AtomicLong(grpParts.size());

        String name = cctx.cache().cacheGroup(grp).cacheOrGroupName();

        CacheGroupContext gctx = cctx.kernalContext().cache().cacheGroup(grp);
        AffinityTopologyVersion topVer = gctx.topology().lastTopologyChangeVersion();

        log.info("Start group dump [name=" + name + ", id=" + grp + ']');

        return grpParts.stream().map(part -> future(() -> {
            long entriesCnt0 = 0;

            try (PartitionDumpContext dumpCtx = dumpCtxs.get(toLong(grp, part))) {
                try (GridCloseableIterator<CacheDataRow> rows = gctx.offheap().reservedIterator(part, topVer)) {
                    if (rows == null)
                        throw new IgniteCheckedException("Partition missing [part=" + part + ']');

                    while (rows.hasNext()) {
                        CacheDataRow row = rows.next();

                        assert row.partition() == part;

                        int cache = row.cacheId() == 0 ? grp : row.cacheId();

                        boolean written = dumpCtx.writeForIterator(cache, row.expireTime(), row.key(), row.value());

                        if (written)
                            entriesCnt0++;
                        else if (log.isTraceEnabled())
                            log.trace("Entry saved by change listener. Skip [" +
                                "grp=" + grp +
                                ", cache=" + cache +
                                ", key=" + row.key() + ']');

                        if (log.isTraceEnabled())
                            log.trace("Row [key=" + row.key() + ", cacheId=" + cache + ']');
                    }
                }

                entriesCnt.addAndGet(entriesCnt0);
                changedEntriesCnt.addAndGet(dumpCtx.changedSize());

                long remain = partsRemain.decrementAndGet();

                if (remain == 0) {
                    log.info("Finish group dump [name=" + name +
                        ", id=" + grp +
                        ", time=" + (System.currentTimeMillis() - start) +
                        ", iteratorEntriesCount=" + entriesCnt +
                        ", changedEntriesCount=" + changedEntriesCnt + ']');
                }
                else if (log.isDebugEnabled()) {
                    log.debug("Finish group partition dump [name=" + name +
                        ", id=" + grp +
                        ", part=" + part +
                        ", time=" + (System.currentTimeMillis() - start) +
                        ", iteratorEntriesCount=" + entriesCnt +
                        ", changedEntriesCount=" + changedEntriesCnt + ']');

                }
            }
        })).collect(Collectors.toList());
    }

    /** {@inheritDoc} */
    @Override public void beforeChange(GridCacheContext cctx, KeyCacheObject key, CacheObject val, long expireTime) {
        assert key.partition() != -1;

        PartitionDumpContext dumpCtx = dumpCtxs.get(toLong(cctx.groupId(), key.partition()));

        assert dumpCtx != null;

        String reasonToSkip = dumpCtx.writeChanged(cctx.cacheId(), expireTime, key, val);

        if (reasonToSkip != null && log.isInfoEnabled()) {
            log.info("Skip entry [grp=" + cctx.groupId() +
                ", cache=" + cctx.cacheId() +
                ", key=" + key +
                ", reason=" + reasonToSkip + ']');
        }

    }

    /** {@inheritDoc} */
    @Override protected CompletableFuture<Void> closeAsync() {
        dumpCtxs.values().stream().forEach(PartitionDumpContext::close);

        if (closeFut == null) {
            Throwable err0 = err.get();

            Set<GroupPartitionId> taken = new HashSet<>();

            for (Map.Entry<Integer, Set<Integer>> e : processed.entrySet()) {
                int grp = e.getKey();

                for (Integer part : e.getValue())
                    taken.add(new GroupPartitionId(grp, part));
            }

            closeFut = CompletableFuture.runAsync(
                () -> onDone(new SnapshotFutureTaskResult(taken, null), err0),
                cctx.kernalContext().pools().getSystemExecutorService()
            );
        }

        return closeFut;
    }

    /** */
    private void createDumpLock() throws IgniteCheckedException, IOException {
        File lock = new File(IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx), DUMP_LOCK);

        if (!lock.createNewFile())
            throw new IgniteCheckedException("Lock file can't be created or already exists: " + lock.getAbsolutePath());
    }

    /**
     * Context of dump single partition.
     */
    private class PartitionDumpContext implements Closeable {
        /** Group id. */
        final int grp;

        /** Partition id. */
        final int part;

        /** Hashes of keys of entries changed by the user during partition dump. */
        final Set<Integer> changed;

        /** Partition dump file. Lazily initialized to prevent creation files for empty partitions. */
        final FileIO file;

        /** Partition serializer. */
        DumpEntrySerializer serdes;

        /** If {@code true} context is closed. */
        volatile boolean closed;

        /**
         * @param grp Group id.
         * @param part Partition id.
         * @param dumpFile Dump file path.
         */
        public PartitionDumpContext(int grp, int part, File dumpFile) throws IOException {
            this.grp = grp;
            this.part = part;

            serdes = new DumpEntrySerializer();
            changed = new GridConcurrentHashSet<>();

            if (!dumpFile.createNewFile())
                throw new IgniteException("Dump file can't be created: " + dumpFile);

            file = ioFactory.create(dumpFile);
        }

        /**
         * Writes entry that changed while dump creation in progress.
         * @param cache Cache id.
         * @param expireTime Expire time.
         * @param key Key.
         * @param val Value
         * @return {@code null} if entry saved in dump or reason why it skipped.
         */
        public synchronized String writeChanged(
            int cache,
            long expireTime,
            KeyCacheObject key,
            CacheObject val
        ) {
            if (closed) // Partition already saved in dump.
                return "partition already saved";
            else if (!changed.add(key.hashCode())) // Entry changed several time during dump.
                return "changed several times";
            else if (val == null)
                return "newly created"; // Previous value is null. Entry created after dump start, skip.

            write(cache, expireTime, key, val);

            return null;
        }

        /**
         * Writes entry fetched from partition iterator.
         * @param cache Cache id.
         * @param expireTime Expire time.
         * @param key Key.
         * @param val Value.
         * @return {@code True} if entry was written in dump,
         *     {@code false} if it already written by {@link #writeChanged(int, long, KeyCacheObject, CacheObject)}.
         */
        public synchronized boolean writeForIterator(
            int cache,
            long expireTime,
            KeyCacheObject key,
            CacheObject val
        ) {
            if (closed)
                throw new IgniteException("Already closed");

            if (changed.contains(key.hashCode()))
                return false;

            write(cache, expireTime, key, val);

            return true;
        }

        /** */
        private void write(int cache, long expireTime, KeyCacheObject key, CacheObject val) {
            assert !closed;

            try {
                ByteBuffer buf = serdes.writeToBuffer(cache, expireTime, key, val, cctx.cacheObjectContext(cache));

                if (file.writeFully(buf) != buf.limit())
                    throw new IgniteException("Can't write row");
            }
            catch (IOException | IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public synchronized void close() {
            if (closed)
                return;

            closed = true;

            U.closeQuiet(file);

            serdes = null;
        }

        /** @return Cound of entries changed while partition dumped. */
        public synchronized long changedSize() {
            return changed.size();
        }
    }

    /** */
    public static long toLong(int high, int low) {
        return (((long)high) << Integer.SIZE) | (low & 0xffffffffL);
    }

    /** */
    private File groupDirectory(CacheGroupContext grpCtx) throws IgniteCheckedException {
        return new File(
            IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx),
            (grpCtx.caches().size() > 1 ? CACHE_GRP_DIR_PREFIX : CACHE_DIR_PREFIX) + grpCtx.cacheOrGroupName()
        );
    }
}
