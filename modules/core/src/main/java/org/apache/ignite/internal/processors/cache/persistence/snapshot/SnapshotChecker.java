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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.cache.IdleVerifyResultV2;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.managers.encryption.EncryptionCacheKeyProvider;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.managers.encryption.GroupKeyEncrypted;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.FLAG_IDX;
import static org.apache.ignite.internal.pagemem.PageIdAllocator.INDEX_PARTITION;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.OWNING;
import static org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState.fromOrdinal;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.CACHE_DATA_FILENAME;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.FILE_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.ZIP_SUFFIX;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheDirectories;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheGroupName;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cachePartitionFiles;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.partId;
import static org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId.getTypeByPartId;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.SNAPSHOT_METAFILE_EXT;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.databaseRelativePath;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.CreateDumpFutureTask.DUMP_FILE_EXT;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.calculatePartitionHash;
import static org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility.checkPartitionsPageCrcSum;

/** */
public class SnapshotChecker {
    /** */
    protected final IgniteLogger log;

    /** */
    @Nullable protected final GridKernalContext kctx;

    /** */
    protected final Marshaller marshaller;

    /** */
    @Nullable protected final ClassLoader marshallerClsLdr;

    /** */
    protected final EncryptionSpi encryptionSpi;

    /** */
    @Nullable protected final CompressionProcessor compression;

    /** */
    protected final ExecutorService executor;

    /** */
    public SnapshotChecker(
        GridKernalContext kctx,
        Marshaller marshaller,
        ExecutorService executorSrvc,
        @Nullable ClassLoader marshallerClsLdr
    ) {
        this.kctx = kctx;

        this.marshaller = marshaller;
        this.marshallerClsLdr = marshallerClsLdr;

        this.encryptionSpi = kctx.config().getEncryptionSpi() == null ? new NoopEncryptionSpi() : kctx.config().getEncryptionSpi();

        this.compression = kctx.compress();

        this.executor = executorSrvc;

        this.log = kctx.log(getClass());
    }

    /** */
    protected List<SnapshotMetadata> readSnapshotMetadatas(File snpFullPath, @Nullable Object nodeConstId) {
        if (!(snpFullPath.exists() && snpFullPath.isDirectory()))
            return Collections.emptyList();

        List<File> smfs = new ArrayList<>();

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(snpFullPath.toPath())) {
            for (Path d : ds) {
                if (Files.isRegularFile(d) && d.getFileName().toString().toLowerCase().endsWith(SNAPSHOT_METAFILE_EXT))
                    smfs.add(d.toFile());
            }
        }
        catch (IOException e) {
            throw new IgniteException(e);
        }

        if (smfs.isEmpty())
            return Collections.emptyList();

        Map<String, SnapshotMetadata> metasMap = new HashMap<>();
        SnapshotMetadata prev = null;

        try {
            for (File smf : smfs) {
                SnapshotMetadata curr = readSnapshotMetadata(smf);

                if (prev != null && !prev.sameSnapshot(curr)) {
                    throw new IgniteException("Snapshot metadata files are from different snapshots " +
                        "[prev=" + prev + ", curr=" + curr + ']');
                }

                metasMap.put(curr.consistentId(), curr);

                prev = curr;
            }
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }

        SnapshotMetadata currNodeSmf = nodeConstId == null ? null : metasMap.remove(nodeConstId.toString());

        // Snapshot metadata for the local node must be first in the result map.
        if (currNodeSmf == null)
            return new ArrayList<>(metasMap.values());
        else {
            List<SnapshotMetadata> result = new ArrayList<>();

            result.add(currNodeSmf);
            result.addAll(metasMap.values());

            return result;
        }
    }

    /** */
    public SnapshotMetadata readSnapshotMetadata(File smf)
        throws IgniteCheckedException, IOException {
        SnapshotMetadata meta = readFromFile(smf);

        String smfName = smf.getName().substring(0, smf.getName().length() - SNAPSHOT_METAFILE_EXT.length());

        if (!U.maskForFileName(meta.consistentId()).equals(smfName)) {
            throw new IgniteException("Error reading snapshot metadata [smfName=" + smfName + ", consId="
                + U.maskForFileName(meta.consistentId()));
        }

        return meta;
    }

    /** */
    public <T> T readFromFile(File smf)
        throws IOException, IgniteCheckedException {
        if (!smf.exists())
            throw new IgniteCheckedException("Snapshot metafile cannot be read due to it doesn't exist: " + smf);

        try (InputStream in = new BufferedInputStream(Files.newInputStream(smf.toPath()))) {
            return marshaller.unmarshal(in, marshallerClsLdr);
        }
    }

    /** Launches local metas checking and waits for the result, handles execution exceptions. */
    public List<SnapshotMetadata> checkLocalMetasResult(File snpPath, @Nullable Collection<Integer> grpIds, @Nullable Object locNodeCstId) {
        try {
            return checkLocalMetas(snpPath, grpIds, locNodeCstId).get();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to check snapshot metadatas of snapshot '" + snpPath.getName() + "'.", e);
        }
    }

    /** Launches local metas checking. */
    public CompletableFuture<List<SnapshotMetadata>> checkLocalMetas(File snpPath, @Nullable Collection<Integer> grpIds,
        @Nullable Object locNodeCstId) {
        return CompletableFuture.supplyAsync(() -> {
            List<SnapshotMetadata> snpMetas = readSnapshotMetadatas(snpPath, locNodeCstId);

            for (SnapshotMetadata meta : snpMetas) {
                byte[] snpMasterKeyDigest = meta.masterKeyDigest();

                if (encryptionSpi.masterKeyDigest() == null && snpMasterKeyDigest != null) {
                    throw new IllegalStateException("Snapshot '" + meta.snapshotName() + "' has encrypted caches " +
                        "while encryption is disabled. To restore this snapshot, start Ignite with configured " +
                        "encryption and the same master key.");
                }

                if (snpMasterKeyDigest != null && !Arrays.equals(snpMasterKeyDigest, encryptionSpi.masterKeyDigest())) {
                    throw new IllegalStateException("Snapshot '" + meta.snapshotName() + "' has different master " +
                        "key digest. To restore this snapshot, start Ignite with the same master key.");
                }

                Collection<Integer> grpIdsToFind = new HashSet<>(F.isEmpty(grpIds) ? meta.cacheGroupIds() : grpIds);

                if (meta.hasCompressedGroups() && grpIdsToFind.stream().anyMatch(meta::isGroupWithCompression)) {
                    try {
                        compression.checkPageCompressionSupported();
                    }
                    catch (NullPointerException | IgniteCheckedException e) {
                        String grpWithCompr = grpIdsToFind.stream().filter(meta::isGroupWithCompression)
                            .map(String::valueOf).collect(Collectors.joining(", "));

                        String msg = "Requested cache groups [" + grpWithCompr + "] for check " +
                            "from snapshot '" + meta.snapshotName() + "' are compressed while " +
                            "disk page compression is disabled. To check these groups please " +
                            "start Ignite with ignite-compress module in classpath";

                        throw new IllegalStateException(msg);
                    }
                }

                grpIdsToFind.removeAll(meta.partitions().keySet());

                if (!grpIdsToFind.isEmpty() && !new HashSet<>(meta.cacheGroupIds()).containsAll(grpIdsToFind)) {
                    throw new IllegalArgumentException("Cache group(s) was not found in the snapshot [groups=" + grpIdsToFind +
                        ", snapshot=" + meta.snapshotName() + ']');
                }
            }

            return snpMetas;
        }, executor);
    }

    /** */
    public static IdleVerifyResultV2 reduceHashesResults(
        Map<ClusterNode, Map<PartitionKeyV2, PartitionHashRecordV2>> results,
        Map<ClusterNode, Exception> ex
    ) {
        Map<PartitionKeyV2, List<PartitionHashRecordV2>> clusterHashes = new HashMap<>();

        results.forEach((node, nodeHashes) -> {
            assert ex.get(node) == null;

            for (Map.Entry<PartitionKeyV2, PartitionHashRecordV2> e : nodeHashes.entrySet()) {
                List<PartitionHashRecordV2> records = clusterHashes.computeIfAbsent(e.getKey(), k -> new ArrayList<>());

                records.add(e.getValue());
            }
        });

        if (results.size() != ex.size())
            return new IdleVerifyResultV2(clusterHashes, ex);
        else
            return new IdleVerifyResultV2(ex);
    }

    /** */
    public static Map<ClusterNode, Exception> reduceMetasResults(
        String snpName,
        @Nullable String snpPath,
        Map<ClusterNode, List<SnapshotMetadata>> allMetas,
        @Nullable Map<ClusterNode, Exception> exceptions,
        Object curNodeCstId
    ) {
        Map<ClusterNode, Exception> mappedExceptions = F.isEmpty(exceptions) ? Collections.emptyMap() : new HashMap<>(exceptions);

        SnapshotMetadata firstMeta = null;
        Set<String> baselineNodes = Collections.emptySet();

        for (Map.Entry<ClusterNode, List<SnapshotMetadata>> nme : allMetas.entrySet()) {
            ClusterNode node = nme.getKey();
            Exception e = mappedExceptions.get(node);

            if (e != null) {
                mappedExceptions.put(node, e);

                continue;
            }

            for (SnapshotMetadata meta : nme.getValue()) {
                if (firstMeta == null) {
                    firstMeta = meta;

                    baselineNodes = new HashSet<>(firstMeta.baselineNodes());
                }

                baselineNodes.remove(meta.consistentId());

                if (!firstMeta.sameSnapshot(meta)) {
                    mappedExceptions.put(node, new IgniteException("An error occurred during comparing snapshot metadata "
                        + "from cluster nodes [firstMeta=" + firstMeta + ", meta=" + meta + ", nodeId=" + node.id() + ']'));
                }
            }
        }

        if (firstMeta == null && mappedExceptions.isEmpty()) {
            assert !allMetas.isEmpty();

            throw new IllegalArgumentException("Snapshot does not exists [snapshot=" + snpName
                + (snpPath != null ? ", baseDir=" + snpPath : "") + ", consistentId=" + curNodeCstId + ']');
        }

        if (!F.isEmpty(baselineNodes) && F.isEmpty(exceptions)) {
            throw new IgniteException("No snapshot metadatas found for the baseline nodes " +
                "with consistent ids: " + String.join(", ", baselineNodes));
        }

        return mappedExceptions;
    }

    /** */
    private IgniteBiTuple<Map<Integer, File>, Set<File>> preparePartitions(SnapshotMetadata meta, Collection<Integer> grps, File snpDir) {
        Map<Integer, File> grpDirs = new HashMap<>();
        Set<File> partFiles = new HashSet<>();

        Set<Integer> grpsLeft = new HashSet<>(F.isEmpty(grps) ? meta.partitions().keySet() : grps);

        for (File dir : cacheDirectories(new File(snpDir, databaseRelativePath(meta.folderName())), name -> true)) {
            int grpId = CU.cacheId(cacheGroupName(dir));

            if (!grpsLeft.remove(grpId))
                continue;

            grpDirs.put(grpId, dir);

            Set<Integer> parts = new HashSet<>(meta.partitions().get(grpId) == null ? Collections.emptySet()
                : meta.partitions().get(grpId));

            for (File partFile : cachePartitionFiles(dir,
                (meta.dump() ? DUMP_FILE_EXT : FILE_SUFFIX) + (meta.compressPartitions() ? ZIP_SUFFIX : "")
            )) {
                int partId = partId(partFile.getName());

                if (!parts.remove(partId))
                    continue;

                partFiles.add(partFile);
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
     * Calls all the registered Absence validaton handlers. Reads snapshot metadata.
     *
     * @see IgniteSnapshotManager#handlers()
     */
    public CompletableFuture<Map<String, SnapshotHandlerResult<Object>>> invokeCustomHandlers(
        String snpName,
        String consId,
        @Nullable String snpPath,
        @Nullable Collection<String> groups,
        boolean check
    ) throws IgniteCheckedException, IOException {
        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        File snpDir = snpMgr.snapshotLocalDir(snpName, snpPath);

        SnapshotMetadata meta = snpMgr.readSnapshotMetadata(snpDir, consId);

        return invokeCustomHandlers(meta, snpPath, groups, check);
    }

    /**
     *  Reads snapshot metadata. Requires snapshot meta to work.
     *
     * @see IgniteSnapshotManager#handlers()
     */
    public CompletableFuture<Map<String, SnapshotHandlerResult<Object>>> invokeCustomHandlers(
        SnapshotMetadata meta,
        @Nullable String snpPath,
        @Nullable Collection<String> groups,
        boolean check
    ) {
        IgniteSnapshotManager snpMgr = kctx.cache().context().snapshotMgr();

        File snpDir = snpMgr.snapshotLocalDir(meta.snapshotName(), snpPath);

        // The handlers use or may use the same snapshot pool. If it configured with 1 thread, launching waiting task in
        // the same pool might block it.
        return CompletableFuture.supplyAsync(() -> {
            try {
                return snpMgr.handlers().invokeAll(SnapshotHandlerType.RESTORE,
                    new SnapshotHandlerContext(meta, groups, kctx.cluster().get().localNode(), snpDir, false, check));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to call custom snapshot validation handlers.", e);
            }
        });
    }

    /** */
    public Map<PartitionKeyV2, PartitionHashRecordV2> checkSnapshotFiles(
        File snpDir,
        Set<Integer> grpIds,
        SnapshotMetadata meta,
        boolean forCreation,
        boolean procPartitionsData,
        boolean skipHash
    ) throws IgniteCheckedException {
        IgniteBiTuple<Map<Integer, File>, Set<File>> grpAndPartFiles = preparePartitions(meta, grpIds, snpDir);

        if (!procPartitionsData) {
            log.info("Snapshot data integrity check skipped [snpName=" + meta.snapshotName() + ']');

            return Collections.emptyMap();
        }

        boolean pouchHoleEnabled = isPunchHoleEnabled(meta, snpDir, grpIds);

        Map<PartitionKeyV2, PartitionHashRecordV2> res = new ConcurrentHashMap<>();
        ThreadLocal<ByteBuffer> buff = ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(meta.pageSize())
            .order(ByteOrder.nativeOrder()));

        FilePageStoreManager storeMgr = (FilePageStoreManager)kctx.cache().context().pageStore();

        EncryptionCacheKeyProvider snpEncrKeyProvider = new SnapshotEncryptionKeyProvider(kctx, grpAndPartFiles.get1());

        try {
            U.doInParallel(
                executor,
                grpAndPartFiles.get2(),
                part -> {
                    String grpName = cacheGroupName(part.getParentFile());
                    int grpId = CU.cacheId(grpName);
                    int partId = partId(part.getName());

                    try (FilePageStore pageStore =
                             (FilePageStore)storeMgr.getPageStoreFactory(grpId, snpEncrKeyProvider.getActiveKey(grpId) != null ?
                                 snpEncrKeyProvider : null).createPageStore(getTypeByPartId(partId), part::toPath, val -> {})
                    ) {
                        pageStore.init();

                        if (pouchHoleEnabled && meta.isGroupWithCompression(grpId) && forCreation) {
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
                            });
                        }

                        if (partId == INDEX_PARTITION) {
                            if (!skipHash)
                                checkPartitionsPageCrcSum(() -> pageStore, INDEX_PARTITION, FLAG_IDX);

                            return null;
                        }

                        if (grpId == MetaStorage.METASTORAGE_CACHE_ID) {
                            if (!skipHash)
                                checkPartitionsPageCrcSum(() -> pageStore, partId, FLAG_DATA);

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
                        PartitionKeyV2 key = new PartitionKeyV2(grpId, partId, grpName);

                        PartitionHashRecordV2 hash = calculatePartitionHash(key,
                            updateCntr,
                            meta.consistentId(),
                            GridDhtPartitionState.OWNING,
                            false,
                            size,
                            skipHash ? F.emptyIterator()
                                : kctx.cache().context().snapshotMgr().partitionRowIterator(kctx, grpName, partId, pageStore));

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

    /** Launches local partitions checking and waits for the result, handles execution exceptions. */
    public Map<PartitionKeyV2, PartitionHashRecordV2> checkPartitionsResult(
        SnapshotMetadata meta,
        File snpDir,
        @Nullable Collection<String> groups,
        boolean forCreation,
        boolean checkParts,
        boolean skipPartsHashes
    ) {
        try {
            return checkPartitions(meta, snpDir, groups, forCreation, checkParts, skipPartsHashes).get();
        }
        catch (Exception e) {
            throw new IgniteException("Failed to get result of partitions validation of snapshot '" + meta.snapshotName() + "'.", e);
        }
    }

    /** Launches local partitions checking. */
    public CompletableFuture<Map<PartitionKeyV2, PartitionHashRecordV2>> checkPartitions(
        SnapshotMetadata meta,
        File snpDir,
        @Nullable Collection<String> groups,
        boolean forCreation,
        boolean checkParts,
        boolean skipPartsHashes
    ) {
        // Await in the default executor to avoid blocking the snapshot executor if it has just one thread.
        return CompletableFuture.supplyAsync(() -> {
            if (!snpDir.exists())
                throw new IllegalStateException("Snapshot directory doesn't exists: " + snpDir);

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
                return checkDumpFiles(snpDir, meta, grps, locNode.consistentId(), checkParts, skipPartsHashes);

            try {
                return checkSnapshotFiles(snpDir, grps, meta, forCreation, checkParts, skipPartsHashes);
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException("Failed to check partitions of snapshot '" + meta.snapshotName() + "'.", e);
            }
        });
    }

    /**
     * Checks results of the internal and custon snapshot validation handlres. Throws exception if a validation error occurs.
     *
     * @see #invokeCustomHandlers(String, String, String, Collection, boolean)
     */
    public void checkCustomHandlersResults(
        String snpName,
        Map<ClusterNode, Map<String, SnapshotHandlerResult<?>>> results
    ) throws Exception {
        Map<String, List<SnapshotHandlerResult<?>>> clusterResults = new HashMap<>();
        Collection<UUID> execNodes = new ArrayList<>(results.size());

        for (Map.Entry<ClusterNode, Map<String, SnapshotHandlerResult<?>>> nodeRes : results.entrySet()) {
            ClusterNode node = nodeRes.getKey();

            // Depending on the job mapping, we can get several different results from one node.
            execNodes.add(node.id());

            assert nodeRes.getValue() != null : "At least the default snapshot restore handler should have been executed ";

            for (Map.Entry<String, SnapshotHandlerResult<?>> nodeHndRes : nodeRes.getValue().entrySet()) {
                String hndName = nodeHndRes.getKey();
                SnapshotHandlerResult<?> hndRes = nodeHndRes.getValue();

                if (hndRes.error() != null)
                    throw hndRes.error();

                clusterResults.computeIfAbsent(hndName, v -> new ArrayList<>()).add(hndRes);
            }
        }

        kctx.cache().context().snapshotMgr().handlers().completeAll(SnapshotHandlerType.RESTORE, snpName, clusterResults,
            execNodes, wrns -> {});
    }

    /** */
    private boolean isPunchHoleEnabled(SnapshotMetadata meta, File snpDir, Set<Integer> grpIds) {
        Path snapshotDir = snpDir.toPath();

        if (meta.hasCompressedGroups() && grpIds.stream().anyMatch(meta::isGroupWithCompression)) {
            try {
                kctx.compress().checkPageCompressionSupported(snapshotDir, meta.pageSize());

                return true;
            }
            catch (Exception e) {
                log.info("File system doesn't support page compression on snapshot directory: " + snapshotDir
                    + ", snapshot may have larger size than expected.");
            }
        }

        return false;
    }

    /** */
    private Map<PartitionKeyV2, PartitionHashRecordV2> checkDumpFiles(
        File snpDir,
        SnapshotMetadata meta,
        Collection<Integer> grpIds,
        Object nodeCstId,
        boolean procPartitionsData,
        boolean skipHash
    ) {
        IgniteBiTuple<Map<Integer, File>, Set<File>> grpAndPartFiles = preparePartitions(meta, grpIds, snpDir);

        if (!procPartitionsData) {
            log.info("Dump data integrity check skipped [dmpName=" + meta.snapshotName() + ']');

            return Collections.emptyMap();
        }

        EncryptionSpi encSpi = meta.encryptionKey() != null ? encryptionSpi : null;

        try (Dump dump = new Dump(snpDir, U.maskForFileName(nodeCstId.toString()), true, true, encSpi, log)) {
            Collection<PartitionHashRecordV2> partitionHashRecordV2s = U.doInParallel(
                executor,
                grpAndPartFiles.get2(),
                part -> calculateDumpedPartitionHash(dump, cacheGroupName(part.getParentFile()), partId(part.getName()),
                    skipHash, nodeCstId, U.maskForFileName(nodeCstId.toString()))
            );

            return partitionHashRecordV2s.stream().collect(Collectors.toMap(PartitionHashRecordV2::partitionKey, r -> r));
        }
        catch (Throwable t) {
            log.error("An unexpected error occurred during dump partitions verifying.", t);

            throw new IgniteException(t);
        }
    }

    /** */
    public static PartitionHashRecordV2 calculateDumpedPartitionHash(Dump dump, String grpName, int part, boolean skipHash,
        Object nodeConstId, String nodeFolderName) {
        if (skipHash) {
            return new PartitionHashRecordV2(
                new PartitionKeyV2(CU.cacheId(grpName), part, grpName),
                false,
                nodeConstId,
                null,
                0,
                PartitionHashRecordV2.PartitionState.OWNING,
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
    public static PartitionHashRecordV2 calculateDumpPartitionHash(
        Dump.DumpedPartitionIterator iter,
        String grpName,
        int part,
        Object consistentId
    ) throws IgniteCheckedException {
        long size = 0;

        IdleVerifyUtility.VerifyPartitionContext ctx = new IdleVerifyUtility.VerifyPartitionContext();

        while (iter.hasNext()) {
            DumpEntry e = iter.next();

            ctx.update((KeyCacheObject)e.key(), (CacheObject)e.value(), e.version());

            size++;
        }

        return new PartitionHashRecordV2(
            new PartitionKeyV2(CU.cacheId(grpName), part, grpName),
            false,
            consistentId,
            null,
            size,
            PartitionHashRecordV2.PartitionState.OWNING,
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
                    p -> Files.isRegularFile(p) && p.toString().endsWith(CACHE_DATA_FILENAME))) {
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
