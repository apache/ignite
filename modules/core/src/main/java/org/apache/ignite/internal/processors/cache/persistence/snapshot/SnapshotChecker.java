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
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.dump.DumpEntry;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.management.cache.IdleVerifyResult;
import org.apache.ignite.internal.management.cache.PartitionKeyV2;
import org.apache.ignite.internal.managers.encryption.EncryptionCacheKeyProvider;
import org.apache.ignite.internal.managers.encryption.GroupKey;
import org.apache.ignite.internal.managers.encryption.GroupKeyEncrypted;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.GridCacheOperation;
import org.apache.ignite.internal.processors.cache.GridLocalConfigManager;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.processors.cache.StoredCacheData;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionState;
import org.apache.ignite.internal.processors.cache.persistence.CacheDataRow;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStore;
import org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.dump.Dump;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.BPlusMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PageIO;
import org.apache.ignite.internal.processors.cache.persistence.tree.io.PagePartitionMetaIO;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileWriteAheadLogManager;
import org.apache.ignite.internal.processors.cache.persistence.wal.reader.IgniteWalIteratorFactory;
import org.apache.ignite.internal.processors.cache.verify.IdleVerifyUtility;
import org.apache.ignite.internal.processors.cache.verify.PartitionHashRecordV2;
import org.apache.ignite.internal.processors.cache.verify.TransactionsHashRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.processors.cluster.BaselineTopology;
import org.apache.ignite.internal.processors.compress.CompressionProcessor;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerUtils;
import org.apache.ignite.spi.encryption.EncryptionSpi;
import org.apache.ignite.spi.encryption.noop.NoopEncryptionSpi;
import org.apache.ignite.transactions.TransactionState;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.managers.discovery.ConsistentIdMapper.ALL_NODES;
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
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.incrementalSnapshotWalsDir;
import static org.apache.ignite.internal.processors.cache.persistence.snapshot.IgniteSnapshotManager.snapshotMetaFileName;
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
    protected final EncryptionSpi encryptionSpi;

    /** */
    protected final ExecutorService executor;

    /** */
    public SnapshotChecker(
        GridKernalContext kctx,
        Marshaller marshaller,
        ExecutorService executorSrvc
    ) {
        this.kctx = kctx;

        this.encryptionSpi = kctx.config().getEncryptionSpi() == null ? new NoopEncryptionSpi() : kctx.config().getEncryptionSpi();

        this.executor = executorSrvc;

        this.log = kctx.log(getClass());
    }

    /** */
    protected List<SnapshotMetadata> readSnapshotMetadatas(File snpFullPath, Object nodeConstId) {
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
                SnapshotMetadata curr = kctx.cache().context().snapshotMgr().readSnapshotMetadata(smf);

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

    /** Launches local metas checking. */
    public CompletableFuture<List<SnapshotMetadata>> checkLocalMetas(
        File snpDir,
        int incIdx,
        @Nullable Collection<Integer> grpIds,
        Object consId
    ) {
        return CompletableFuture.supplyAsync(() -> {
            List<SnapshotMetadata> snpMetas = readSnapshotMetadatas(snpDir, consId);

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
                        kctx.compress().checkPageCompressionSupported();
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

            if (incIdx > 0) {
                List<SnapshotMetadata> metas = snpMetas.stream().filter(m -> m.consistentId().equals(String.valueOf(consId)))
                    .collect(Collectors.toList());

                if (metas.size() != 1) {
                    throw new IgniteException("Failed to find single snapshot metafile on local node [locNodeId="
                        + consId + ", metas=" + snpMetas + ", snpName=" + snpDir.getName()
                        + ", snpPath=" + snpDir.getParent() + "]. Incremental snapshots requires exactly one meta file " +
                        "per node because they don't support restoring on a different topology.");
                }

                checkIncrementalSnapshotsExist(metas.get(0), snpDir, incIdx);
            }

            return snpMetas;
        }, executor);
    }

    /** Checks that all incremental snapshots are present, contain correct metafile and WAL segments. */
    private void checkIncrementalSnapshotsExist(SnapshotMetadata fullMeta, File snpDir, int incIdx) {
        try {
            // Incremental snapshot must contain ClusterSnapshotRecord.
            long startSeg = fullMeta.snapshotRecordPointer().index();

            String snpName = fullMeta.snapshotName();

            for (int inc = 1; inc <= incIdx; inc++) {
                File incSnpDir = kctx.cache().context().snapshotMgr().incrementalSnapshotLocalDir(snpName, snpDir.getParent(), inc);

                if (!incSnpDir.exists()) {
                    throw new IllegalArgumentException("No incremental snapshot found " +
                        "[snpName=" + snpName + ", snpPath=" + snpDir.getParent() + ", incrementIndex=" + inc + ']');
                }

                String metaFileName = snapshotMetaFileName(kctx.pdsFolderResolver().resolveFolders().folderName());

                File metafile = incSnpDir.toPath().resolve(metaFileName).toFile();

                IncrementalSnapshotMetadata incMeta = kctx.cache().context().snapshotMgr().readFromFile(metafile);

                if (!incMeta.matchBaseSnapshot(fullMeta)) {
                    throw new IllegalArgumentException("Incremental snapshot doesn't match full snapshot " +
                        "[incMeta=" + incMeta + ", fullMeta=" + fullMeta + ']');
                }

                if (incMeta.incrementIndex() != inc) {
                    throw new IgniteException(
                        "Incremental snapshot meta has wrong index [expectedIdx=" + inc + ", meta=" + incMeta + ']');
                }

                checkWalSegments(incMeta, startSeg, incrementalSnapshotWalsDir(incSnpDir, incMeta.folderName()));

                // Incremental snapshots must not cross each other.
                startSeg = incMeta.incrementalSnapshotPointer().index() + 1;
            }
        }
        catch (IgniteCheckedException | IOException e) {
            throw new IgniteException(e);
        }
    }

    /** Check that incremental snapshot contains all required WAL segments. Throws {@link IgniteException} in case of any errors. */
    private void checkWalSegments(IncrementalSnapshotMetadata meta, long startWalSeg, File incSnpWalDir) {
        IgniteWalIteratorFactory factory = new IgniteWalIteratorFactory(log);

        List<FileDescriptor> walSeg = factory.resolveWalFiles(
            new IgniteWalIteratorFactory.IteratorParametersBuilder()
                .filesOrDirs(incSnpWalDir.listFiles(file ->
                    FileWriteAheadLogManager.WAL_SEGMENT_FILE_COMPACTED_PATTERN.matcher(file.getName()).matches())));

        if (walSeg.isEmpty())
            throw new IgniteException("No WAL segments found for incremental snapshot [dir=" + incSnpWalDir + ']');

        long actFirstSeg = walSeg.get(0).idx();

        if (actFirstSeg != startWalSeg) {
            throw new IgniteException("Missed WAL segment [expectFirstSegment=" + startWalSeg
                + ", actualFirstSegment=" + actFirstSeg + ", meta=" + meta + ']');
        }

        long expLastSeg = meta.incrementalSnapshotPointer().index();
        long actLastSeg = walSeg.get(walSeg.size() - 1).idx();

        if (actLastSeg != expLastSeg) {
            throw new IgniteException("Missed WAL segment [expectLastSegment=" + startWalSeg
                + ", actualLastSegment=" + actFirstSeg + ", meta=" + meta + ']');
        }

        List<?> walSegGaps = factory.hasGaps(walSeg);

        if (!walSegGaps.isEmpty())
            throw new IgniteException("Missed WAL segments [misses=" + walSegGaps + ", meta=" + meta + ']');
    }

    /** */
    public CompletableFuture<IncrementalSnapshotCheckResult> checkIncrementalSnapshot(
        String snpName,
        @Nullable String snpPath,
        int incIdx
    ) {
        assert incIdx > 0;

        return CompletableFuture.supplyAsync(
            () -> {
                Object consId = kctx.cluster().get().localNode().consistentId();

                File snpDir = kctx.cache().context().snapshotMgr().snapshotLocalDir(snpName, snpPath);

                try {
                    if (log.isInfoEnabled()) {
                        log.info("Verify incremental snapshot procedure has been initiated " +
                            "[snpName=" + snpName + ", incrementIndex=" + incIdx + ", consId=" + consId + ']');
                    }

                    BaselineTopology blt = kctx.state().clusterState().baselineTopology();

                    SnapshotMetadata meta = kctx.cache().context().snapshotMgr().readSnapshotMetadata(snpDir, consId.toString());

                    if (!F.eqNotOrdered(blt.consistentIds().stream().map(Object::toString).collect(Collectors.toList()),
                        meta.baselineNodes())) {
                        throw new IgniteCheckedException("Topologies of snapshot and current cluster are different [snp=" +
                            meta.baselineNodes() + ", current=" + blt.consistentIds() + ']');
                    }

                    Map<Integer, StoredCacheData> txCaches = readTxCachesData(snpDir);

                    AtomicLong procSegCnt = new AtomicLong();

                    LongAdder procEntriesCnt = new LongAdder();

                    IncrementalSnapshotProcessor proc = new IncrementalSnapshotProcessor(
                        kctx.cache().context(), snpName, snpPath, incIdx, txCaches.keySet()
                    ) {
                        @Override void totalWalSegments(int segCnt) {
                            // No-op.
                        }

                        @Override void processedWalSegments(int segCnt) {
                            procSegCnt.set(segCnt);
                        }

                        @Override void initWalEntries(LongAdder entriesCnt) {
                            procEntriesCnt.add(entriesCnt.sum());
                        }
                    };

                    short locNodeId = blt.consistentIdMapping().get(consId);

                    Set<GridCacheVersion> activeDhtTxs = new HashSet<>();
                    Map<GridCacheVersion, Set<Short>> txPrimParticipatingNodes = new HashMap<>();
                    Map<Short, HashHolder> nodesTxHash = new HashMap<>();

                    Set<GridCacheVersion> partiallyCommittedTxs = new HashSet<>();
                    // Hashes in this map calculated based on WAL records only, not part-X.bin data.
                    Map<PartitionKeyV2, HashHolder> partMap = new HashMap<>();
                    List<Exception> exceptions = new ArrayList<>();

                    Function<Short, HashHolder> hashHolderBuilder = (k) -> new HashHolder();

                    BiConsumer<GridCacheVersion, Set<Short>> calcTxHash = (xid, participatingNodes) -> {
                        for (short nodeId : participatingNodes) {
                            if (nodeId != locNodeId) {
                                HashHolder hash = nodesTxHash.computeIfAbsent(nodeId, hashHolderBuilder);

                                hash.increment(xid.hashCode(), 0);
                            }
                        }
                    };

                    // CacheId -> CacheGrpId.
                    Map<Integer, Integer> cacheGrpId = txCaches.values().stream()
                        .collect(Collectors.toMap(
                            StoredCacheData::cacheId,
                            cacheData -> CU.cacheGroupId(cacheData.config().getName(), cacheData.config().getGroupName())
                        ));

                    LongAdder procTxCnt = new LongAdder();

                    proc.process(dataEntry -> {
                        if (dataEntry.op() == GridCacheOperation.READ || !exceptions.isEmpty())
                            return;

                        if (log.isTraceEnabled())
                            log.trace("Checking data entry [entry=" + dataEntry + ']');

                        if (!activeDhtTxs.contains(dataEntry.writeVersion()))
                            partiallyCommittedTxs.add(dataEntry.nearXidVersion());

                        StoredCacheData cacheData = txCaches.get(dataEntry.cacheId());

                        PartitionKeyV2 partKey = new PartitionKeyV2(
                            cacheGrpId.get(dataEntry.cacheId()),
                            dataEntry.partitionId(),
                            CU.cacheOrGroupName(cacheData.config()));

                        HashHolder hash = partMap.computeIfAbsent(partKey, (k) -> new HashHolder());

                        try {
                            int valHash = dataEntry.key().hashCode();

                            if (dataEntry.value() != null)
                                valHash += Arrays.hashCode(dataEntry.value().valueBytes(null));

                            int verHash = dataEntry.writeVersion().hashCode();

                            hash.increment(valHash, verHash);
                        }
                        catch (IgniteCheckedException ex) {
                            exceptions.add(ex);
                        }
                    }, txRec -> {
                        if (!exceptions.isEmpty())
                            return;

                        if (log.isDebugEnabled())
                            log.debug("Checking tx record [txRec=" + txRec + ']');

                        if (txRec.state() == TransactionState.PREPARED) {
                            // Collect only primary nodes. For some cases backup nodes is included into TxRecord#participationNodes()
                            // but actually doesn't even start transaction, for example, if the node participates only as a backup
                            // of reading only keys.
                            Set<Short> primParticipatingNodes = txRec.participatingNodes().keySet();

                            if (primParticipatingNodes.contains(locNodeId)) {
                                txPrimParticipatingNodes.put(txRec.nearXidVersion(), primParticipatingNodes);
                                activeDhtTxs.add(txRec.writeVersion());
                            }
                            else {
                                for (Collection<Short> backups : txRec.participatingNodes().values()) {
                                    if (backups.contains(ALL_NODES) || backups.contains(locNodeId))
                                        activeDhtTxs.add(txRec.writeVersion());
                                }
                            }
                        }
                        else if (txRec.state() == TransactionState.COMMITTED) {
                            activeDhtTxs.remove(txRec.writeVersion());

                            Set<Short> participatingNodes = txPrimParticipatingNodes.remove(txRec.nearXidVersion());

                            // Legal cases:
                            // 1. This node is a transaction near node, but not primary or backup node.
                            // 2. This node participated in the transaction multiple times (e.g., primary for one key and
                            //    backup for other key).
                            // 3. A transaction is included into previous incremental snapshot.
                            if (participatingNodes == null)
                                return;

                            procTxCnt.increment();

                            calcTxHash.accept(txRec.nearXidVersion(), participatingNodes);
                        }
                        else if (txRec.state() == TransactionState.ROLLED_BACK) {
                            activeDhtTxs.remove(txRec.writeVersion());
                            txPrimParticipatingNodes.remove(txRec.nearXidVersion());
                        }
                    });

                    // All active transactions that didn't log COMMITTED or ROLL_BACK records are considered committed.
                    // It is possible as incremental snapshot started after transaction left IgniteTxManager#activeTransactions()
                    // collection, but completed before the final TxRecord was written.
                    for (Map.Entry<GridCacheVersion, Set<Short>> tx : txPrimParticipatingNodes.entrySet())
                        calcTxHash.accept(tx.getKey(), tx.getValue());

                    Map<Object, TransactionsHashRecord> txHashRes = nodesTxHash.entrySet().stream()
                        .map(e -> new TransactionsHashRecord(
                            consId,
                            blt.compactIdMapping().get(e.getKey()),
                            e.getValue().hash
                        ))
                        .collect(Collectors.toMap(
                            TransactionsHashRecord::remoteConsistentId,
                            Function.identity()
                        ));

                    Map<PartitionKeyV2, PartitionHashRecordV2> partHashRes = partMap.entrySet().stream()
                        .collect(Collectors.toMap(
                            Map.Entry::getKey,
                            e -> new PartitionHashRecordV2(
                                e.getKey(),
                                false,
                                consId,
                                null,
                                0,
                                null,
                                new IdleVerifyUtility.VerifyPartitionContext(e.getValue().hash, e.getValue().verHash)
                            )
                        ));

                    if (log.isInfoEnabled()) {
                        log.info("Verify incremental snapshot procedure finished " +
                            "[snpName=" + snpName + ", incrementIndex=" + incIdx + ", consId=" + consId +
                            ", txCnt=" + procTxCnt.sum() + ", dataEntries=" + procEntriesCnt.sum() +
                            ", walSegments=" + procSegCnt.get() + ']');
                    }

                    return new IncrementalSnapshotCheckResult(
                        txHashRes,
                        partHashRes,
                        partiallyCommittedTxs,
                        exceptions
                    );
                }
                catch (IgniteCheckedException | IOException e) {
                    throw new IgniteException(e);
                }
            },
            executor
        );
    }

    /** @return Collection of snapshotted transactional caches, key is a cache ID. */
    private Map<Integer, StoredCacheData> readTxCachesData(File snpDir) throws IgniteCheckedException, IOException {
        String folderName = kctx.pdsFolderResolver().resolveFolders().folderName();

        return GridLocalConfigManager.readCachesData(
                new File(snpDir, databaseRelativePath(folderName)),
                MarshallerUtils.jdkMarshaller(kctx.igniteInstanceName()),
                kctx.config())
            .values().stream()
            .filter(data -> data.config().getAtomicityMode() == CacheAtomicityMode.TRANSACTIONAL)
            .collect(Collectors.toMap(StoredCacheData::cacheId, Function.identity()));
    }

    /** */
    public IdleVerifyResult reduceIncrementalResults(
        Map<ClusterNode, List<IncrementalSnapshotCheckResult>> results,
        Map<ClusterNode, Exception> operationErrors
    ) {
        if (!operationErrors.isEmpty())
            return IdleVerifyResult.builder().exceptions(operationErrors).build();

        IdleVerifyResult.Builder bldr = IdleVerifyResult.builder();

        results.forEach((node, resLst) -> resLst.forEach(incSnpNodeRes->{
            if(incSnpNodeRes.)
        });


                results.forEach((node, resLst) -> resLst.forEach(res -> {
            if (res.exceptions().isEmpty() && !bldr.hasErrors()) {
                if (!F.isEmpty(res.partiallyCommittedTxs()))
                    bldr.addPartiallyCommited(node, res.partiallyCommittedTxs());

                bldr.addPartitionHashes(res.partHashRes());

                if (log.isDebugEnabled())
                    log.debug("Handle VerifyIncrementalSnapshotJob result [node=" + node + ", taskRes=" + res + ']');

                nodeTxHashMap.put(node.consistentId(), res.txHashRes());

                Iterator<Map.Entry<Object, TransactionsHashRecord>> resIt = res.txHashRes().entrySet().iterator();

                while (resIt.hasNext()) {
                    Map.Entry<Object, TransactionsHashRecord> nodeTxHash = resIt.next();

                    Map<Object, TransactionsHashRecord> prevNodeTxHash = nodeTxHashMap.get(nodeTxHash.getKey());

                    if (prevNodeTxHash != null) {
                        TransactionsHashRecord hash = nodeTxHash.getValue();
                        TransactionsHashRecord prevHash = prevNodeTxHash.remove(hash.localConsistentId());

                        if (prevHash == null || prevHash.transactionHash() != hash.transactionHash())
                            bldr.addTxConflicts(F.asList(hash, prevHash));

                        resIt.remove();
                    }
                }
            }
            else if (!res.exceptions().isEmpty())
                bldr.addException(node, F.first(res.exceptions()));
        }));

        return bldr.build();
    }

    /** */
    public static Map<ClusterNode, Exception> reduceMetasResults(
        String snpName,
        @Nullable String snpPath,
        Map<ClusterNode, List<SnapshotMetadata>> allMetas,
        @Nullable Map<ClusterNode, Exception> exceptions,
        Object consId
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
            throw new IllegalArgumentException("Snapshot does not exists [snapshot=" + snpName
                + (snpPath != null ? ", baseDir=" + snpPath : "") + ", consistentId=" + consId + ']');
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
     * Calls all the registered custom validaton handlers. Reads snapshot metadata.
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
     * Calls all the registered custom validaton handlers.
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

        boolean checkCompressed = forCreation && isPunchHoleEnabled(meta, snpDir, grpIds);

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

                        GridIterator<CacheDataRow> partIt = skipHash
                            ? F.emptyIterator()
                            : kctx.cache().context().snapshotMgr().partitionRowIterator(kctx, grpName, partId, pageStore,
                                kctx.cache().context());

                        PartitionHashRecordV2 hash = calculatePartitionHash(key,
                            updateCntr,
                            meta.consistentId(),
                            GridDhtPartitionState.OWNING,
                            false,
                            size,
                            partIt
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
     * Checks results of all the snapshot validation handlres.
     * @param snpName Snapshot name
     * @param results Results: checking node -> snapshot part's consistend id -> custom handler name -> handler result.
     * @see #invokeCustomHandlers(String, String, String, Collection, boolean)
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
        Object consId,
        boolean procPartitionsData,
        boolean skipHash
    ) {
        IgniteBiTuple<Map<Integer, File>, Set<File>> grpAndPartFiles = preparePartitions(meta, grpIds, snpDir);

        if (!procPartitionsData) {
            log.info("Dump data integrity check skipped [dmpName=" + meta.snapshotName() + ']');

            return Collections.emptyMap();
        }

        EncryptionSpi encSpi = meta.encryptionKey() != null ? encryptionSpi : null;

        try (Dump dump = new Dump(snpDir, consId.toString(), true, true, encSpi, log)) {
            String nodeFolderName = kctx.pdsFolderResolver().resolveFolders().folderName();

            Collection<PartitionHashRecordV2> partitionHashRecordV2s = U.doInParallel(
                executor,
                grpAndPartFiles.get2(),
                part -> calculateDumpedPartitionHash(dump, cacheGroupName(part.getParentFile()), partId(part.getName()),
                    skipHash, consId, nodeFolderName)
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
        Object consId
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
            consId,
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

    /** Holder for calculated hashes. */
    private static class HashHolder {
        /** */
        private int hash;

        /** */
        private int verHash;

        /** */
        private void increment(int hash, int verHash) {
            this.hash += hash;
            this.verHash += verHash;
        }
    }
}
