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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.MarshallerContextImpl;
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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.AbstractCreateBackupFutureTask;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotFutureTaskResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotSender;
import org.apache.ignite.internal.processors.marshaller.MappedName;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.lang.GridCloseableIterator;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.GridLocalConfigManager.cachDataFilename;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_GRP_DIR_PREFIX;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.DUMP_LOCK;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.DumpEntrySerializer.FILE_VER;

/** */
public class DumpCacheFutureTask extends AbstractCreateBackupFutureTask implements DumpEntryChangeListener {
    /** Dump files name. */
    public static final String DUMP_FILE_NAME = "dump.bin";

    /** */
    private final File dumpDir;

    /** */
    private final FileIOFactory ioFactory;

    /** Group -> Partition -> Set of changed entries. */
    private final Map<Integer, Map<Integer, Set<Integer>>> changedEntries = new HashMap<>();

    /** */
    private final Map<Integer, FileIO> dumpFiles = new HashMap<>();

    /** */
    private final Map<Integer, DumpEntrySerializer> serializers = new HashMap<>();

    /**
     * @param cctx Cache context.
     * @param srcNodeId Node id which cause snapshot task creation.
     * @param reqId Snapshot operation request ID.
     * @param dumpName Dump name.
     * @param ioFactory IO factory.
     * @param snpSndr Snapshot sender.
     * @param parts Parts to dump.
     */
    public DumpCacheFutureTask(
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

            File dumpNodeDir = IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx);

            createDumpLock(dumpNodeDir);

            processPartitions();

            initAll();

            backupAllAsync();
        }
        catch (IgniteCheckedException | IOException e) {
            acceptException(e);

            onDone(e);
        }

        return false; // Don't wait for checkpoint.
    }

    /** Prepares all data structures to dump entries. */
    private void initAll() throws IOException, IgniteCheckedException {
        for (int grp : processed.keySet()) {
            serializers.put(grp, DumpEntrySerializer.serializer(FILE_VER));

            File grpDumpDir = groupDirectory(cctx.cache().cacheGroup(grp));

            if (!grpDumpDir.mkdirs())
                throw new IgniteCheckedException("Dump directory can't be created: " + grpDumpDir);

            File dumpFile = new File(grpDumpDir, DUMP_FILE_NAME);

            if (!dumpFile.createNewFile())
                throw new IgniteCheckedException("Dump file can't be created: " + dumpFile);

            dumpFiles.put(grp, ioFactory.create(dumpFile));

            byte[] fileVer = U.shortToBytes(FILE_VER);

            if (dumpFiles.get(grp).writeFully(fileVer, 0, fileVer.length) != fileVer.length)
                throw new IgniteCheckedException("Can't write file version");

            CacheGroupContext gctx = cctx.cache().cacheGroup(grp);

            Map<Integer, Set<Integer>> partsMap = new HashMap<>();

            for (int part : processed.get(grp))
                partsMap.put(part, new HashSet<>());

            changedEntries.put(grp, partsMap);

            for (GridCacheContext cctx : gctx.caches())
                cctx.dumpListener(this);
        }
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveMetaCopy() {
        Collection<BinaryType> types = cctx.kernalContext().cacheObjects().binary().types();

        ArrayList<Map<Integer, MappedName>> mappings = cctx.kernalContext().marshallerContext().getCachedMappings();

        return Arrays.asList(
            CompletableFuture.runAsync(
                wrapExceptionIfStarted(
                    () -> cctx.kernalContext().cacheObjects().saveMetadata(types, dumpDir)
                ),
                snpSndr.executor()
            ),

            CompletableFuture.runAsync(
                wrapExceptionIfStarted(() -> MarshallerContextImpl.saveMappings(cctx.kernalContext(), mappings, dumpDir)),
                snpSndr.executor()
            )
        );
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveCacheConfigsCopy() {
        return parts.keySet().stream().map(grp -> CompletableFuture.runAsync(wrapExceptionIfStarted(() -> {
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
        }), snpSndr.executor())).collect(Collectors.toList());
    }

    /** */
    private File groupDirectory(CacheGroupContext grpCtx) throws IgniteCheckedException {
        return new File(
            IgniteSnapshotManager.nodeDumpDirectory(dumpDir, cctx),
            (grpCtx.caches().size() > 1 ? CACHE_GRP_DIR_PREFIX : CACHE_DIR_PREFIX) + grpCtx.cacheOrGroupName()
        );
    }

    /** {@inheritDoc} */
    @Override protected List<CompletableFuture<Void>> saveGroup(int grp, Set<Integer> grpParts) throws IgniteCheckedException {
        return Collections.singletonList(CompletableFuture.runAsync(wrapExceptionIfStarted(() -> {
            long start = System.currentTimeMillis();
            long entriesCnt = 0;
            long changedEntriesCnt = 0;

            CacheGroupContext grpCtx = cctx.cache().cacheGroup(grp);

            log.info("Start group dump [name=" + grpCtx.cacheOrGroupName() + ", id=" + grp + ']');

            CacheGroupContext gctx = cctx.kernalContext().cache().cacheGroup(grp);
            AffinityTopologyVersion topVer = gctx.topology().lastTopologyChangeVersion();
            FileIO dumpFile = dumpFiles.get(grp);

            try {
                for (int part : grpParts) {
                    try (GridCloseableIterator<CacheDataRow> rows = gctx.offheap().reservedIterator(part, topVer)) {
                        if (rows == null)
                            throw new IgniteCheckedException("Partition missing [part=" + part + ']');

                        while (rows.hasNext()) {
                            CacheDataRow row = rows.next();

                            int cache = row.cacheId() == 0 ? grp : row.cacheId();

                            writeRow(grp, cache, row.partition(), row.expireTime(), row.key(), row.value(), true);

                            entriesCnt++;

                            if (log.isTraceEnabled())
                                log.trace("row [key=" + row.key() + ", cacheId=" + cache + ']');
                        }
                    }

                    synchronized (dumpFile) {
                        Map<Integer, Set<Integer>> grpMap = changedEntries.get(grp);

                        changedEntriesCnt += grpMap.remove(part).size();

                        if (grpMap.isEmpty())
                            changedEntries.remove(grp);
                    }
                }
            }
            catch (IOException e) {
                throw new IgniteCheckedException(e);
            }
            finally {
                U.closeQuiet(dumpFile);
            }

            log.info("Finish group dump [name=" + grpCtx.cacheOrGroupName() +
                ", id=" + grp +
                ", time=" + (System.currentTimeMillis() - start) +
                ", iteratorEntriesCount=" + entriesCnt +
                ", changedEntriesCount=" + changedEntriesCnt + ']');
        }), snpSndr.executor()));
    }

    /** */
    private void writeRow(
        int grp,
        int cache,
        int partition,
        long expireTime,
        KeyCacheObject key,
        CacheObject val,
        boolean fromIter
    ) throws IgniteCheckedException, IOException {
        FileIO dumpFile = dumpFiles.get(grp);

        synchronized (dumpFile) {
            String reasonToSkip = isSkip(grp, partition, key, val, fromIter);

            if (reasonToSkip != null) {
                if (log.isTraceEnabled())
                    log.trace("Skip entry [grp=" + grp +
                        ", cache=" + cache +
                        ", key=" + key +
                        ", iter=" + fromIter +
                        ", reason=" + reasonToSkip + ']');

                return; // Entry already stored. Skip.
            }

            if (log.isTraceEnabled())
                log.trace("Dumping entry [grp=" + grp +
                    ", cache=" + cache +
                    ", key=" + key +
                    ", iter=" + fromIter + ']');

            DumpEntrySerializer serializer = serializers.get(grp);

            ByteBuffer buf = serializer.writeToBuffer(cache, partition, expireTime, key, val, cctx.cacheObjectContext(cache));

            if (dumpFile.writeFully(buf) != buf.limit())
                throw new IgniteCheckedException("Can't write row");
        }
    }

    /**
     * @return Reason to skip entry. Or {@code null} is row must be dumped.
     */
    private String isSkip(int grp, int partition, KeyCacheObject key, CacheObject val, boolean iterator) {
        Map<Integer, Set<Integer>> grpMap = changedEntries.get(grp);

        if (iterator) {
            if (grpMap.get(partition).contains(key.hashCode()))
                return "already saved";
        }
        else {
            if (grpMap == null) // Group already saved in dump.
                return "group already saved";
            else {
                Set<Integer> changed = grpMap.get(partition);

                if (changed == null) // Partition already saved in dump.
                    return "partition not found";
                else {
                    if (!changed.add(key.hashCode())) // Entry changed several time during dump.
                        return "changed several times";
                    else if (val == null)
                        return "newly created"; // Previous value is null. Entry created after dump start, skip.
                }
            }
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void beforeChange(GridCacheContext cctx, KeyCacheObject key, CacheObject val, long expireTime) {
        assert key.partition() != -1;

        try {
            writeRow(cctx.groupId(), cctx.cacheId(), key.partition(), expireTime, key, val, false);
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override protected CompletableFuture<Void> closeAsync() {
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
    private void createDumpLock(File dumpNodeDir) throws IgniteCheckedException, IOException {
        File lock = new File(dumpNodeDir, DUMP_LOCK);

        if (!lock.createNewFile())
            throw new IgniteCheckedException("Lock file can't be created or already exists: " + lock.getAbsolutePath());
    }
}
