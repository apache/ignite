/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.persistence.snapshot;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.management.cache.PartitionKey;
import org.apache.ignite.internal.managers.encryption.EncryptionCacheKeyProvider;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.managers.encryption.GroupKeyEncrypted;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.VerifyPartitionContext;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.fromOrdinal;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.cacheName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.partId;
import static org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree.cachePartitionFiles;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.closeAllComponents;
import static org.apache.ignite.internal.processors.cache.persistence.wal.reader.StandaloneGridKernalContext.startAllComponents;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.calculatePartitionHash;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.checkPartitionsPageCrcSum;

/**
 * Default snapshot restore handler for checking snapshot partitions consistency.
 */
public class SnapshotPartitionsVerifyHandler implements SnapshotHandler<Map<PartitionKey, PartitionHashRecord>> {
    /** Shared context. */
    protected final GridCacheSharedContext<?, ?> cctx;

    /** Logger. */
    private final IgniteLogger log;

    /** @param cctx Shared context. */
    public SnapshotPartitionsVerifyHandler(GridCacheSharedContext<?, ?> cctx) {
        this.cctx = cctx;

        log = cctx.logger(getClass());
    }

    /** {@inheritDoc} */
    @Override public SnapshotHandlerType type() {
        return SnapshotHandlerType.RESTORE;
    }

    /** {@inheritDoc} */
    @Override public Map<PartitionKey, PartitionHashRecord> invoke(SnapshotHandlerContext opCtx) throws IgniteCheckedException {
        if (!opCtx.snapshotFileTree().root().exists())
            throw new IgniteCheckedException("Snapshot directory doesn't exists: " + opCtx.snapshotFileTree().root());

        SnapshotMetadata meta = opCtx.metadata();

        Set<Integer> grps = F.isEmpty(opCtx.groups())
            ? new HashSet<>(meta.partitions().keySet())
            : opCtx.groups().stream().map(CU::cacheId).collect(Collectors.toSet());

        if (type() == SnapshotHandlerType.CREATE) {
            grps = grps.stream().filter(grp -> grp == MetaStorage.METASTORAGE_CACHE_ID ||
                CU.affinityNode(
                    cctx.localNode(),
                    cctx.kernalContext().cache().cacheGroupDescriptor(grp).config().getNodeFilter()
                )
            ).collect(Collectors.toSet());
        }

        Set<File> partFiles = new HashSet<>();

        Map<Integer, File> grpDirs = new HashMap<>();

        for (File dir : opCtx.snapshotFileTree().cacheDirectories(name -> true)) {
            int grpId = CU.cacheId(cacheName(dir));

            if (!grps.remove(grpId))
                continue;

            Set<Integer> parts = meta.partitions().get(grpId) == null ? Collections.emptySet() :
                new HashSet<>(meta.partitions().get(grpId));

            for (File part : cachePartitionFiles(dir, meta.dump(), meta.compressPartitions())) {
                int partId = partId(part);

                if (!parts.remove(partId))
                    continue;

                partFiles.add(part);
            }

            if (!parts.isEmpty()) {
                throw new IgniteException("Snapshot data doesn't contain required cache group partition " +
                    "[grpId=" + grpId + ", snpName=" + meta.snapshotName() + ", consId=" + meta.consistentId() +
                    ", missed=" + parts + ", meta=" + meta + ']');
            }

            grpDirs.put(grpId, dir);
        }

        if (!grps.isEmpty()) {
            throw new IgniteException("Snapshot data doesn't contain required cache groups " +
                "[grps=" + grps + ", snpName=" + meta.snapshotName() + ", consId=" + meta.consistentId() +
                ", meta=" + meta + ']');
        }

        if (!opCtx.check()) {
            log.info("Snapshot data integrity check skipped [snpName=" + meta.snapshotName() + ']');

            return Collections.emptyMap();
        }

        return meta.dump()
            ? checkDumpFiles(opCtx, partFiles)
            : checkSnapshotFiles(opCtx, grpDirs, meta, partFiles, isPunchHoleEnabled(opCtx, grpDirs.keySet()));
    }

    /** */
    private Map<PartitionKey, PartitionHashRecord> checkSnapshotFiles(
        SnapshotHandlerContext opCtx,
        Map<Integer, File> grpDirs,
        SnapshotMetadata meta,
        Set<File> partFiles,
        boolean punchHoleEnabled
    ) throws IgniteCheckedException {
        Map<PartitionKey, PartitionHashRecord> res = new ConcurrentHashMap<>();
        ThreadLocal<ByteBuffer> buff = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(meta.pageSize())
            .order(ByteOrder.nativeOrder()));

        IgniteSnapshotManager snpMgr = cctx.snapshotMgr();

        GridKernalContext snpCtx = snpMgr.createStandaloneKernalContext(cctx.kernalContext().compress(),
            opCtx.snapshotFileTree().root(), meta.folderName());

        FilePageStoreManager storeMgr = (FilePageStoreManager)cctx.pageStore();

        EncryptionCacheKeyProvider snpEncrKeyProvider = new SnapshotEncryptionKeyProvider(cctx.kernalContext(), grpDirs);

        startAllComponents(snpCtx);

        try {
            U.doInParallel(
                snpMgr.snapshotExecutorService(),
                partFiles,
                part -> {
                    String grpName = cacheName(part.getParentFile());
                    int grpId = CU.cacheId(grpName);
                    int partId = partId(part);

                    try (FilePageStore pageStore =
                             (FilePageStore)storeMgr.getPageStoreFactory(grpId, snpEncrKeyProvider.getActiveKey(grpId) != null ?
                                 snpEncrKeyProvider : null).createPageStore(getTypeByPartId(partId), part::toPath, val -> {})
                    ) {
                        pageStore.init();

                        if (punchHoleEnabled && meta.isGroupWithCompression(grpId) && type() == SnapshotHandlerType.CREATE) {
                            byte pageType = partId == INDEX_PARTITION ? FLAG_IDX : FLAG_DATA;

                            checkPartitionsPageCrcSum(() -> pageStore, partId, pageType, (id, buffer) -> {
                                if (PageIO.getCompressionType(buffer) == CompressionProcessor.UNCOMPRESSED_PAGE)
                                    return;

                                int comprPageSz = PageIO.getCompressedSize(buffer);

                                if (comprPageSz < pageStore.getPageSize()) {
                                    try {
                                        pageStore.punchHole(id, comprPageSz);
                                    }
                                    catch (Exception ignored) {
                                        // No-op.
                                    }
                                }
                            }, null);
                        }

                        if (partId == INDEX_PARTITION) {
                            if (!skipHash())
                                checkPartitionsPageCrcSum(() -> pageStore, INDEX_PARTITION, FLAG_IDX, null);

                            return null;
                        }

                        if (grpId == MetaStorage.METASTORAGE_CACHE_ID) {
                            if (!skipHash())
                                checkPartitionsPageCrcSum(() -> pageStore, partId, FLAG_DATA, null);

                            return null;
                        }

                        ByteBuffer pageBuff = buff.get();
                        pageBuff.clear();
                        pageStore.read(0, pageBuff, true);

                        long pageAddr = GridUnsafe.bufferAddress(pageBuff);

                        if (PageIO.getCompressionType(pageBuff) != CompressionProcessor.UNCOMPRESSED_PAGE)
                            snpCtx.compress().decompressPage(pageBuff, pageStore.getPageSize());

                        PagePartitionMetaIO io = PageIO.getPageIO(pageBuff);
                        GridDhtPartitionState partState = fromOrdinal(io.getPartitionState(pageAddr));

                        if (partState != OWNING) {
                            throw new IgniteCheckedException("Snapshot partitions must be in the OWNING " +
                                "state only: " + partState);
                        }

                        long updateCntr = io.getUpdateCounter(pageAddr);
                        long size = io.getSize(pageAddr);

                        if (log.isDebugEnabled()) {
                            log.debug("Partition [grpId=" + grpId
                                + ", id=" + partId
                                + ", counter=" + updateCntr
                                + ", size=" + size + "]");
                        }

                        // Snapshot partitions must always be in OWNING state.
                        // There is no `primary` partitions for snapshot.
                        PartitionKey key = new PartitionKey(grpId, partId, grpName);

                        PartitionHashRecord hash = calculatePartitionHash(key,
                            updateCntr,
                            meta.consistentId(),
                            GridDhtPartitionState.OWNING,
                            false,
                            size,
                            skipHash() ? F.emptyIterator()
                                : snpMgr.partitionRowIterator(snpCtx, grpName, partId, pageStore),
                            null
                        );

                        assert hash != null : "OWNING must have hash: " + key;

                        // We should skip size comparison if there are entries to expire exist.
                        if (hasExpiringEntries(snpCtx, pageStore, pageBuff, io.getPendingTreeRoot(pageAddr)))
                            hash.hasExpiringEntries(true);

                        res.put(key, hash);
                    }
                    catch (IOException e) {
                        throw new IgniteCheckedException(e);
                    }

                    return null;
                }
            );
        }
        catch (Throwable t) {
            log.error("Error executing handler: ", t);

            throw t;
        }
        finally {
            closeAllComponents(snpCtx);
        }

        return res;
    }

    /** */
    private boolean hasExpiringEntries(
        GridKernalContext ctx,
        PageStore pageStore,
        ByteBuffer pageBuff,
        long pendingTreeMetaId
    ) throws IgniteCheckedException {
        if (pendingTreeMetaId == 0)
            return false;

        long pageAddr = GridUnsafe.bufferAddress(pageBuff);

        pageBuff.clear();
        pageStore.read(pendingTreeMetaId, pageBuff, true);

        if (PageIO.getCompressionType(pageBuff) != CompressionProcessor.UNCOMPRESSED_PAGE)
            ctx.compress().decompressPage(pageBuff, pageStore.getPageSize());

        BPlusMetaIO treeIO = BPlusMetaIO.VERSIONS.forPage(pageAddr);

        int rootLvl = treeIO.getRootLevel(pageAddr);
        long rootId = treeIO.getFirstPageId(pageAddr, rootLvl);

        pageBuff.clear();
        pageStore.read(rootId, pageBuff, true);

        if (PageIO.getCompressionType(pageBuff) != CompressionProcessor.UNCOMPRESSED_PAGE)
            ctx.compress().decompressPage(pageBuff, pageStore.getPageSize());

        BPlusIO<?> rootIO = PageIO.getPageIO(pageBuff);

        return rootIO.getCount(pageAddr) != 0;
    }

    /** */
    private Map<PartitionKey, PartitionHashRecord> checkDumpFiles(
        SnapshotHandlerContext opCtx,
        Set<File> partFiles
    ) {
        EncryptionSpi encSpi = opCtx.metadata().encryptionKey() != null ? cctx.gridConfig().getEncryptionSpi() : null;

        try (Dump dump = new Dump(opCtx.snapshotFileTree().root(), opCtx.snapshotFileTree().consistentId(), true, true, encSpi, log)) {
            Collection<PartitionHashRecord> partitionHashRecords = U.doInParallel(
                cctx.snapshotMgr().snapshotExecutorService(),
                partFiles,
                part -> calculateDumpedPartitionHash(dump, cacheName(part.getParentFile()), partId(part))
            );

            return partitionHashRecords.stream().collect(Collectors.toMap(PartitionHashRecord::partitionKey, r -> r));
        }
        catch (Throwable t) {
            log.error("Error executing handler: ", t);

            throw new IgniteException(t);
        }
    }

    /** */
    private PartitionHashRecord calculateDumpedPartitionHash(Dump dump, String grpName, int part) {
        if (skipHash()) {
            return new PartitionHashRecord(
                new PartitionKey(CU.cacheId(grpName), part, grpName),
                false,
                cctx.localNode().consistentId(),
                null,
                0,
                PartitionHashRecord.PartitionState.OWNING,
                new VerifyPartitionContext()
            );
        }

        try {
            String node = cctx.kernalContext().pdsFolderResolver().resolveFolders().folderName();

            try (Dump.DumpedPartitionIterator iter = dump.iterator(node, CU.cacheId(grpName), part)) {
                long size = 0;

                VerifyPartitionContext ctx = new VerifyPartitionContext();

                while (iter.hasNext()) {
                    DumpEntry e = iter.next();

                    ctx.update((KeyCacheObject)e.key(), (CacheObject)e.value(), e.version());

                    size++;
                }

                return new PartitionHashRecord(
                    new PartitionKey(CU.cacheId(grpName), part, grpName),
                    false,
                    cctx.localNode().consistentId(),
                    null,
                    size,
                    PartitionHashRecord.PartitionState.OWNING,
                    ctx
                );
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public void complete(String name,
        Collection<SnapshotHandlerResult<Map<PartitionKey, PartitionHashRecord>>> results) throws IgniteCheckedException {
        Map<PartitionKey, List<PartitionHashRecord>> clusterHashes = new HashMap<>();
        Map<ClusterNode, Exception> errs = new HashMap<>();

        for (SnapshotHandlerResult<Map<PartitionKey, PartitionHashRecord>> res : results) {
            if (res.error() != null) {
                errs.put(res.node(), res.error());

                continue;
            }

            for (Map.Entry<PartitionKey, PartitionHashRecord> entry : res.data().entrySet())
                clusterHashes.computeIfAbsent(entry.getKey(), v -> new ArrayList<>()).add(entry.getValue());
        }

        IdleVerifyResult verifyResult = new IdleVerifyResult(clusterHashes, errs);

        if (errs.isEmpty() && !verifyResult.hasConflicts())
            return;

        GridStringBuilder buf = new GridStringBuilder();

        verifyResult.print(buf::a, true);

        throw new IgniteCheckedException(buf.toString());
    }

    /**
     * Provides flag of full hash calculation.
     *
     * @return {@code True} if full partition hash calculation is required. {@code False} otherwise.
     */
    protected boolean skipHash() {
        return false;
    }

    /** */
    protected boolean isPunchHoleEnabled(SnapshotHandlerContext opCtx, Set<Integer> grpIds) {
        SnapshotMetadata meta = opCtx.metadata();

        if (meta.hasCompressedGroups() && grpIds.stream().anyMatch(meta::isGroupWithCompression)) {
            try {
                cctx.kernalContext().compress().checkPageCompressionSupported(opCtx.snapshotFileTree().root().toPath(), meta.pageSize());

                return true;
            }
            catch (Exception e) {
                log.info("File system doesn't support page compression on snapshot directory: " + opCtx.snapshotFileTree().root()
                    + ", snapshot may have larger size than expected.");
            }
        }

        return false;
    }

    /**
     * Provides encryption keys stored within snapshot.
     * <p>
     * To restore an encrypted snapshot, we have to read the keys it was encrypted with. The better place for the is
     * Metastore. But it is currently unreadable as simple structure. Once it is done, we should move snapshot
     * encryption keys there.
     */
    private static class SnapshotEncryptionKeyProvider implements EncryptionCacheKeyProvider {
        /** Kernal context */
        private final GridKernalContext ctx;

        /** Data dirs of snapshot's caches by group id. */
        private final Map<Integer, File> grpDirs;

        /** Encryption keys loaded from snapshot. */
        private final ConcurrentHashMap<Integer, GroupKey> decryptedKeys = new ConcurrentHashMap<>();

        /**
         * Constructor.
         *
         * @param ctx     Kernal context.
         * @param grpDirs Data dirictories of cache groups by id.
         */
        private SnapshotEncryptionKeyProvider(GridKernalContext ctx, Map<Integer, File> grpDirs) {
            this.ctx = ctx;
            this.grpDirs = grpDirs;
        }

        /** {@inheritDoc} */
        @Override public @Nullable GroupKey getActiveKey(int grpId) {
            return decryptedKeys.computeIfAbsent(grpId, id -> {
                GroupKey grpKey = null;

                try (DirectoryStream<Path> ds = Files.newDirectoryStream(grpDirs.get(grpId).toPath(),
                    p -> Files.isRegularFile(p) && NodeFileTree.cacheOrCacheGroupConfigFile(p.toFile()))) {
                    for (Path p : ds) {
                        StoredCacheData cacheData = ctx.cache().configManager().readCacheData(p.toFile());

                        GroupKeyEncrypted grpKeyEncrypted = cacheData.groupKeyEncrypted();

                        if (grpKeyEncrypted == null)
                            return null;

                        if (grpKey == null)
                            grpKey = new GroupKey(grpKeyEncrypted.id(), ctx.config().getEncryptionSpi().decryptKey(grpKeyEncrypted.key()));
                        else {
                            assert grpKey.equals(new GroupKey(grpKeyEncrypted.id(),
                                ctx.config().getEncryptionSpi().decryptKey(grpKeyEncrypted.key())));
                        }
                    }

                    return grpKey;
                }
                catch (Exception e) {
                    throw new IgniteException("Unable to extract ciphered encryption key of cache group " + id + '.', e);
                }
            });
        }

        /** {@inheritDoc} */
        @Override public @Nullable GroupKey groupKey(int grpId, int keyId) {
            GroupKey key = getActiveKey(grpId);

            return key != null && key.id() == keyId ? key : null;
        }
    }
}
