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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree;
import org.apache.ignite.internal.processors.cache.persistence.filename.SnapshotFileTree;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecord;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.fromOrdinal;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.cacheName;
import static org.apache.ignite.internal.processors.cache.persistence.filename.NodeFileTree.partId;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.calculatePartitionHash;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.checkPartitionsPageCrcSum;

/** */
public class SnapshotChecker {
    /** */
    private final IgniteLogger log;

    /** */
    private final GridKernalContext kctx;

    /** */
    private final EncryptionSpi encryptionSpi;

    /** */
    private final ExecutorService executor;

    /** */
    public SnapshotChecker(GridKernalContext kctx) {
        this.kctx = kctx;

        encryptionSpi = kctx.config().getEncryptionSpi() == null ? new NoopEncryptionSpi() : kctx.config().getEncryptionSpi();

        executor = kctx.pools().getSnapshotExecutorService();

        log = kctx.log(getClass());
    }

    /** Launches local metas checking. */
    public CompletableFuture<List<SnapshotMetadata>> checkLocalMetas(
        SnapshotFileTree sft,
        int incIdx,
        @Nullable Collection<Integer> grpIds
    ) {
        return CompletableFuture.supplyAsync(() ->
            new SnapshotMetadataVerificationTask(kctx.grid(), log, sft, incIdx, grpIds).execute(), executor);
    }

    /** */
    public CompletableFuture<IncrementalSnapshotCheckResult> checkIncrementalSnapshot(
        SnapshotFileTree sft,
        int incIdx
    ) {
        assert incIdx > 0;

        return CompletableFuture.supplyAsync(
            () -> new IncrementalSnapshotVerificationTask(kctx.grid(), log, sft, incIdx).execute(),
            executor);
    }

    /** */
    public IdleVerifyResult reduceIncrementalResults(
        SnapshotFileTree sft,
        int incIdx,
        Map<ClusterNode, IncrementalSnapshotCheckResult> results,
        Map<ClusterNode, Exception> operationErrors
    ) {
        if (!operationErrors.isEmpty())
            return IdleVerifyResult.builder().exceptions(operationErrors).build();

        return new IncrementalSnapshotVerificationTask(kctx.grid(), log, sft, incIdx).reduce(results);
    }

    /** */
    public Map<ClusterNode, Exception> reduceMetasResults(SnapshotFileTree sft, Map<ClusterNode, List<SnapshotMetadata>> metas) {
        return new SnapshotMetadataVerificationTask(kctx.grid(), log, sft, 0, null).reduce(metas);
    }

    /** */
    private IgniteBiTuple<Map<Integer, File>, Set<File>> preparePartitions(
        SnapshotMetadata meta,
        Collection<Integer> grps,
        SnapshotFileTree sft
    ) {
        Map<Integer, File> grpDirs = new HashMap<>();
        Set<File> partFiles = new HashSet<>();

        Set<Integer> grpsLeft = new HashSet<>(F.isEmpty(grps) ? meta.partitions().keySet() : grps);

        for (File dir : sft.allCacheDirs()) {
            int grpId = CU.cacheId(cacheName(dir));

            if (!grpsLeft.remove(grpId))
                continue;

            grpDirs.put(grpId, dir);

            Set<Integer> parts = new HashSet<>(meta.partitions().get(grpId) == null ? Collections.emptySet()
                : meta.partitions().get(grpId));

            for (File part : sft.cachePartitionFiles(dir, meta.dump(), meta.compressPartitions())) {
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
        }

        if (!grpsLeft.isEmpty()) {
            throw new IgniteException("Snapshot data doesn't contain required cache groups " +
                "[grps=" + grpsLeft + ", snpName=" + meta.snapshotName() + ", consId=" + meta.consistentId() +
                ", meta=" + meta + ']');
        }

        return new IgniteBiTuple<>(grpDirs, partFiles);
    }

    /**
     * Calls all the registered custom validaton handlers.
     *
     * @see IgniteSnapshotManager#handlers()
     */
    public CompletableFuture<Map<String, SnapshotHandlerResult<Object>>> invokeCustomHandlers(
        SnapshotMetadata meta,
        SnapshotFileTree sft,
        @Nullable Collection<String> groups,
        boolean check
    ) {
        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        // The handlers use or may use the same snapshot pool. If it configured with 1 thread, launching waiting task in
        // the same pool might block it.
        return CompletableFuture.supplyAsync(() -> {
            try {
                return snpMgr.handlers().invokeAll(SnapshotHandlerType.RESTORE,
                    new SnapshotHandlerContext(meta, groups, kctx.cluster().get().localNode(), sft, false, check));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to call custom snapshot validation handlers.", e);
            }
        });
    }

    /** */
    public Map<PartitionKey, PartitionHashRecord> checkSnapshotFiles(
        SnapshotFileTree sft,
        Set<Integer> grpIds,
        SnapshotMetadata meta,
        boolean forCreation,
        boolean procPartitionsData,
        boolean skipHash
    ) throws IgniteCheckedException {
        IgniteBiTuple<Map<Integer, File>, Set<File>> grpAndPartFiles = preparePartitions(meta, grpIds, sft);

        if (!procPartitionsData) {
            log.info("Snapshot data integrity check skipped [snpName=" + meta.snapshotName() + ']');

            return Collections.emptyMap();
        }

        boolean checkCompressed = forCreation && isPunchHoleEnabled(meta, sft, grpIds);

        Map<PartitionKey, PartitionHashRecord> res = new ConcurrentHashMap<>();
        ThreadLocal<ByteBuffer> buff = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(meta.pageSize())
            .order(ByteOrder.nativeOrder()));

        FilePageStoreManager storeMgr = (FilePageStoreManager)kctx.cache().context().pageStore();

        EncryptionCacheKeyProvider snpEncrKeyProvider = new SnapshotEncryptionKeyProvider(kctx, grpAndPartFiles.get1());

        try {
            U.doInParallel(
                executor,
                grpAndPartFiles.get2(),
                part -> {
                    String grpName = cacheName(part.getParentFile());
                    int grpId = CU.cacheId(grpName);
                    int partId = partId(part);

                    try (FilePageStore pageStore =
                             (FilePageStore)storeMgr.getPageStoreFactory(grpId, snpEncrKeyProvider.getActiveKey(grpId) != null ?
                                 snpEncrKeyProvider : null).createPageStore(getTypeByPartId(partId), part::toPath, val -> {})
                    ) {
                        pageStore.init();

                        if (checkCompressed && meta.isGroupWithCompression(grpId)) {
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
                            if (!skipHash)
                                checkPartitionsPageCrcSum(() -> pageStore, INDEX_PARTITION, FLAG_IDX, null);

                            return null;
                        }

                        if (grpId == MetaStorage.METASTORAGE_CACHE_ID) {
                            if (!skipHash)
                                checkPartitionsPageCrcSum(() -> pageStore, partId, FLAG_DATA, null);

                            return null;
                        }

                        ByteBuffer pageBuff = buff.get();
                        pageBuff.clear();
                        pageStore.read(0, pageBuff, true);

                        long pageAddr = GridUnsafe.bufferAddress(pageBuff);

                        if (PageIO.getCompressionType(pageBuff) != CompressionProcessor.UNCOMPRESSED_PAGE)
                            kctx.compress().decompressPage(pageBuff, pageStore.getPageSize());

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

                        GridIterator<CacheDataRow> partIt = skipHash
                            ? F.emptyIterator()
                            : kctx.cache().context().snapshotMgr().partitionRowIterator(kctx, grpName, partId, pageStore,
                                kctx.cache().context());

                        PartitionHashRecord hash = calculatePartitionHash(key,
                            updateCntr,
                            meta.consistentId(),
                            GridDhtPartitionState.OWNING,
                            false,
                            size,
                            partIt,
                            null
                        );

                        assert hash != null : "OWNING must have hash: " + key;

                        // We should skip size comparison if there are entries to expire exist.
                        if (hasExpiringEntries(kctx, pageStore, pageBuff, io.getPendingTreeRoot(pageAddr)))
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
            log.error("An error occurred on snapshot partitions validation.", t);

            throw t;
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

    /** Launches local partitions checking. */
    public CompletableFuture<Map<PartitionKey, PartitionHashRecord>> checkPartitions(
        SnapshotMetadata meta,
        SnapshotFileTree sft,
        @Nullable Collection<String> groups,
        boolean forCreation,
        boolean checkParts,
        boolean skipPartsHashes
    ) {
        // Await in the default executor to avoid blocking the snapshot executor if it has just one thread.
        return CompletableFuture.supplyAsync(() -> {
            if (!sft.root().exists())
                throw new IllegalStateException("Snapshot directory doesn't exists: " + sft.root());

            ClusterNode locNode = kctx.cluster().get().localNode();

            Set<Integer> grps = F.isEmpty(groups)
                ? new HashSet<>(meta.partitions().keySet())
                : groups.stream().map(CU::cacheId).collect(Collectors.toSet());

            if (forCreation) {
                grps = grps.stream().filter(grp -> grp == MetaStorage.METASTORAGE_CACHE_ID ||
                    CU.affinityNode(
                        locNode,
                        kctx.cache().cacheGroupDescriptor(grp).config().getNodeFilter()
                    )
                ).collect(Collectors.toSet());
            }

            if (meta.dump())
                return checkDumpFiles(sft, meta, grps, locNode.consistentId(), checkParts, skipPartsHashes);

            try {
                return checkSnapshotFiles(sft, grps, meta, forCreation, checkParts, skipPartsHashes);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to check partitions of snapshot '" + meta.snapshotName() + "'.", e);
            }
        });
    }

    /**
     * Checks results of all the snapshot validation handlres.
     * @param snpName Snapshot name
     * @param results Results: checking node -> snapshot part's consistend id -> custom handler name -> handler result.
     * @see #invokeCustomHandlers(SnapshotMetadata, SnapshotFileTree, Collection, boolean)
     */
    public void checkCustomHandlersResults(
        String snpName,
        Map<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> results
    ) throws Exception {
        Map<String, List<SnapshotHandlerResult<?>>> clusterResults = new HashMap<>();
        Collection<UUID> execNodes = new ArrayList<>(results.size());

        // Checking node -> Map by snapshot part's consistend id.
        for (Map.Entry<ClusterNode, Map<Object, Map<String, SnapshotHandlerResult<?>>>> nodeRes : results.entrySet()) {
            // Consistent id -> Map by handler name.
            for (Map.Entry<Object, Map<String, SnapshotHandlerResult<?>>> nodeConsIdRes : nodeRes.getValue().entrySet()) {
                ClusterNode node = nodeRes.getKey();

                // We can get several different results from one node.
                execNodes.add(node.id());

                assert nodeRes.getValue() != null : "At least the default snapshot restore handler should have been executed ";

                // Handler name -> handler result.
                for (Map.Entry<String, SnapshotHandlerResult<?>> nodeHndRes : nodeConsIdRes.getValue().entrySet()) {
                    String hndName = nodeHndRes.getKey();
                    SnapshotHandlerResult<?> hndRes = nodeHndRes.getValue();

                    if (hndRes.error() != null)
                        throw hndRes.error();

                    clusterResults.computeIfAbsent(hndName, v -> new ArrayList<>()).add(hndRes);
                }
            }
        }

        kctx.cache().context().snapshotMgr().handlers().completeAll(SnapshotHandlerType.RESTORE, snpName, clusterResults,
            execNodes, wrns -> {});
    }

    /** */
    private boolean isPunchHoleEnabled(SnapshotMetadata meta, SnapshotFileTree sft, Set<Integer> grpIds) {
        if (meta.hasCompressedGroups() && grpIds.stream().anyMatch(meta::isGroupWithCompression)) {
            try {
                kctx.compress().checkPageCompressionSupported(sft.root().toPath(), meta.pageSize());

                return true;
            }
            catch (Exception e) {
                log.info("File system doesn't support page compression on snapshot directory: " + sft.root()
                    + ", snapshot may have larger size than expected.");
            }
        }

        return false;
    }

    /** */
    private Map<PartitionKey, PartitionHashRecord> checkDumpFiles(
        SnapshotFileTree sft,
        SnapshotMetadata meta,
        Collection<Integer> grpIds,
        Object consId,
        boolean procPartitionsData,
        boolean skipHash
    ) {
        IgniteBiTuple<Map<Integer, File>, Set<File>> grpAndPartFiles = preparePartitions(meta, grpIds, sft);

        if (!procPartitionsData) {
            log.info("Dump data integrity check skipped [dmpName=" + meta.snapshotName() + ']');

            return Collections.emptyMap();
        }

        EncryptionSpi encSpi = meta.encryptionKey() != null ? encryptionSpi : null;

        try (Dump dump = new Dump(sft.root(), consId.toString(), true, true, encSpi, log)) {
            String nodeFolderName = kctx.pdsFolderResolver().resolveFolders().folderName();

            Collection<PartitionHashRecord> partitionHashRecordV2s = U.doInParallel(
                executor,
                grpAndPartFiles.get2(),
                part -> calculateDumpedPartitionHash(dump, cacheName(part.getParentFile()), partId(part),
                    skipHash, consId, nodeFolderName)
            );

            return partitionHashRecordV2s.stream().collect(Collectors.toMap(PartitionHashRecord::partitionKey, r -> r));
        }
        catch (Throwable t) {
            log.error("An unexpected error occurred during dump partitions verifying.", t);

            throw new IgniteException(t);
        }
    }

    /** */
    public static PartitionHashRecord calculateDumpedPartitionHash(Dump dump, String grpName, int part, boolean skipHash,
        Object nodeConstId, String nodeFolderName) {
        if (skipHash) {
            return new PartitionHashRecord(
                new PartitionKey(CU.cacheId(grpName), part, grpName),
                false,
                nodeConstId,
                null,
                0,
                PartitionHashRecord.PartitionState.OWNING,
                new IdleVerifyUtility.VerifyPartitionContext()
            );
        }

        try {
            try (Dump.DumpedPartitionIterator iter = dump.iterator(nodeFolderName, CU.cacheId(grpName), part)) {
                return calculateDumpPartitionHash(iter, grpName, part, nodeConstId);
            }
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }
    }

    /** */
    public static PartitionHashRecord calculateDumpPartitionHash(
        Dump.DumpedPartitionIterator iter,
        String grpName,
        int part,
        Object consId
    ) throws IgniteCheckedException {
        long size = 0;

        IdleVerifyUtility.VerifyPartitionContext ctx = new IdleVerifyUtility.VerifyPartitionContext();

        while (iter.hasNext()) {
            DumpEntry e = iter.next();

            ctx.update((KeyCacheObject)e.key(), (CacheObject)e.value(), e.version());

            size++;
        }

        return new PartitionHashRecord(
            new PartitionKey(CU.cacheId(grpName), part, grpName),
            false,
            consId,
            null,
            size,
            PartitionHashRecord.PartitionState.OWNING,
            ctx
        );
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
