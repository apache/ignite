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

package org.apache.ignite.internal.processors.cache.persistence.preload;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessSupplier;
import org.apache.ignite.internal.processors.cache.persistence.backup.IgniteBackupPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileTransferManager;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.PartitionFileMetaInfo;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.persistence.preload.GridCachePreloadSharedManager.rebalanceThreadTopic;

/**
 *
 */
public class GridPartitionUploadManager {
    /** */
    private GridCacheSharedContext<?, ?> cctx;

    /** */
    private IgniteLogger log;

    /** */
    private final ConcurrentMap<UUID, CachePartitionUploadFuture> uploadFutMap = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private IgniteBackupPageStoreManager backupMgr;

    /**
     * @param ktx Kernal context to process.
     */
    public GridPartitionUploadManager(GridKernalContext ktx) {
        assert CU.isPersistenceEnabled(ktx.config());

        cctx = ktx.cache().context();

        log = ktx.log(getClass());
    }

    /**
     * @return {@code True} if cluster rebalance via sending partition files can be applied.
     */
    static boolean persistenceRebalanceApplicable(GridCacheSharedContext cctx) {
        return !cctx.kernalContext().clientNode() &&
            CU.isPersistenceEnabled(cctx.kernalContext().config()) &&
            cctx.isRebalanceEnabled();
    }

    /**
     * @param cctx Cache shared context.
     */
    void start0(GridCacheSharedContext<?, ?> cctx) throws IgniteCheckedException {
        this.cctx = cctx;

        backupMgr = cctx.storeBackup();

        if (persistenceRebalanceApplicable(cctx)) {
            cctx.gridIO().addMessageListener(rebalanceThreadTopic(), new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                    if (msg instanceof GridPartitionBatchDemandMessage) {
                        // Start to checkpoint and upload process.
                        lock.readLock().lock();

                        try {
                            onDemandMessage0(nodeId, (GridPartitionBatchDemandMessage)msg, plc);
                        }
                        finally {
                            lock.readLock().unlock();
                        }
                    }
                }
            });
        }
    }

    /**
     * @param cancel <tt>true</tt> to cancel all pending tasks.
     */
    void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            cctx.gridIO().removeMessageListener(rebalanceThreadTopic());

            for (CachePartitionUploadFuture fut : uploadFutMap.values())
                fut.cancel();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * This internal method will handle demand requests of copying cache group partitions to the remote node.
     * It will perform checkpoint operation to take the latest partitions changes for list of demaned cache
     * groups and upload them one by one.
     *
     * @param nodeId The nodeId request comes from.
     * @param msg Message containing rebalance request params.
     */
    private void onDemandMessage0(UUID nodeId, GridPartitionBatchDemandMessage msg, byte plc) {
        if (msg.rebalanceId() < 0) // Demand node requested context cleanup.
            return;

        ClusterNode demanderNode = cctx.discovery().node(nodeId);

        if (demanderNode == null) {
            U.error(log, "The demand message rejected (demander node left the cluster) ["
                + ", nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']');

            return;
        }

        if (msg.assignments() == null || msg.assignments().isEmpty()) {
            U.error(log, "The Demand message rejected. Node assignments cannot be empty ["
                + "nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']');

            return;
        }

        IgniteSocketChannel ch = null;
        CachePartitionUploadFuture uploadFut = null;

        try {
            synchronized (uploadFutMap) {
                uploadFut = uploadFutMap.getOrDefault(nodeId,
                    new CachePartitionUploadFuture(msg.rebalanceId(), msg.topologyVersion(), msg.assignments()));

                if (uploadFut.rebalanceId < msg.rebalanceId()) {
                    if (!uploadFut.isDone())
                        uploadFut.cancel();

                    uploadFutMap.put(nodeId,
                        uploadFut = new CachePartitionUploadFuture(msg.rebalanceId(),
                            msg.topologyVersion(),
                            msg.assignments()));
                }
            }

            // Need to start new partition upload routine.
            ch = cctx.gridIO().channelToCustomTopic(nodeId, rebalanceThreadTopic(), null, plc);

            backupMgr.backup(uploadFut.rebalanceId,
                uploadFut.getAssigns(),
                new SocketBackupProcessSupplier(
                    new FileTransferManager<>(cctx.kernalContext(), ch.channel(), uploadFut),
                    log
                ),
                uploadFut);

            uploadFut.onDone(true);
        }
        catch (Exception e) {
            U.error(log, "An error occured while processing initial demand request ["
                + ", nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']', e);

            if (uploadFut != null)
                uploadFut.onDone(e);
        }
        finally {
            U.closeQuiet(ch);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridPartitionUploadManager.class, this);
    }

    /** */
    private static class SocketBackupProcessSupplier implements BackupProcessSupplier {
        /** */
        private final FileTransferManager<PartitionFileMetaInfo> ftMrg;

        /** */
        private final IgniteLogger log;

        /**
         * @param ftMrg An upload helper class.
         */
        public SocketBackupProcessSupplier(FileTransferManager<PartitionFileMetaInfo> ftMrg, IgniteLogger log) {
            this.ftMrg = ftMrg;
            this.log = log;
        }

        /** {@inheritDoc} */
        @Override public void supplyPartition(
            GroupPartitionId grpPartId,
            File file,
            long size
        ) throws IgniteCheckedException {
            U.log(log, "Start partition meta info uploading: " + grpPartId);

            ftMrg.writeMetaFrom(new PartitionFileMetaInfo(grpPartId.getGroupId(),
                grpPartId.getPartitionId(),
                file.getName(),
                size,
                0));

            U.log(log, "Start partition uploading: " + file.getName());

            ftMrg.writeFrom(file, 0, size);
        }

        /** {@inheritDoc} */
        @Override public void supplyDelta(
            GroupPartitionId grpPartId,
            File file,
            long offset,
            long size
        ) throws IgniteCheckedException {
            U.log(log, "Start delta meta info uploading: " + grpPartId);

            ftMrg.writeMetaFrom(new PartitionFileMetaInfo(grpPartId.getGroupId(),
                grpPartId.getPartitionId(),
                file.getName(),
                size,
                1));

            U.log(log, "Start delta uploading: " + file.getName());

            ftMrg.writeFrom(file, offset, size);
        }
    }

    /** */
    private static class CachePartitionUploadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private long rebalanceId;

        /** */
        private AffinityTopologyVersion topVer;

        /** */
        private Map<Integer, GridIntList> assigns;

        /** */
        public CachePartitionUploadFuture(
            long rebalanceId,
            AffinityTopologyVersion topVer,
            Map<Integer, GridIntList> assigns
        ) {
            this.rebalanceId = rebalanceId;
            this.topVer = topVer;
            this.assigns = assigns;
        }

        /**
         * @return The map of assignments of each cache group.
         */
        public Map<Integer, Set<Integer>> getAssigns() {
            Map<Integer, Set<Integer>> result = new HashMap<>();

            for (Map.Entry<Integer, GridIntList> grpPartsEntry : assigns.entrySet()) {
                GridIntIterator iterator = grpPartsEntry.getValue().iterator();

                result.putIfAbsent(grpPartsEntry.getKey(), new HashSet<>());

                while (iterator.hasNext())
                    result.get(grpPartsEntry.getKey()).add(iterator.next());
            }

            return result;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() {
            return onCancelled();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CachePartitionUploadFuture.class, this);
        }
    }
}
