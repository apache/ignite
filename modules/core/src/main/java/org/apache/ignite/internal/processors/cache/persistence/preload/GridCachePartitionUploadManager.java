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
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedManagerAdapter;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessTask;
import org.apache.ignite.internal.processors.cache.persistence.backup.IgniteBackupPageStoreManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileTransferManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.PartitionFileMetaInfo;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.GridIntIterator;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.GridTopic.TOPIC_REBALANCE;

/**
 *
 */
public class GridCachePartitionUploadManager extends GridCacheSharedManagerAdapter {
    /** The default factory to provide IO oprations over uploading files. */
    private static final FileIOFactory dfltIoFactory = new RandomAccessFileIOFactory();

    /** */
    private final ConcurrentMap<T2<UUID, Integer>, CachePartitionUploadFuture> uploadFutMap = new ConcurrentHashMap<>();

    /** */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** */
    private IgniteBackupPageStoreManager backupMgr;

    /**
     * @param ctx The kernal context.
     */
    public GridCachePartitionUploadManager(GridKernalContext ctx) {
        assert CU.isPersistenceEnabled(ctx.config());

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
     * @param idx The index of rebalace topic.
     * @return The Rebalance topic to communicate with.
     */
    static Object rebalanceThreadTopic(int idx) {
        return TOPIC_REBALANCE.topic("Rebalance", idx);
    }

    /** {@inheritDoc} */
    @Override protected void start0() throws IgniteCheckedException {
        backupMgr = cctx.storeBackup();

        if (persistenceRebalanceApplicable(cctx)) {
            for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++) {
                final int topicId = cnt;

                cctx.gridIO().addMessageListener(rebalanceThreadTopic(topicId), new GridMessageListener() {
                    @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                        if (msg instanceof GridPartitionsCopyDemandMessage) {
                            // Start to checkpoint and upload process.
                            if (lock.readLock().tryLock())
                                return;

                            try {
                                onDemandMessage0(nodeId, topicId, (GridPartitionsCopyDemandMessage)msg, plc);
                            }
                            finally {
                                lock.readLock().unlock();
                            }
                        }
                    }
                });
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void stop0(boolean cancel) {
        lock.writeLock().lock();

        try {
            for (int cnt = 0; cnt < cctx.gridConfig().getRebalanceThreadPoolSize(); cnt++)
                cctx.gridIO().removeMessageListener(rebalanceThreadTopic(cnt));

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
     * @param topicId The index of rebalance pool thread.
     * @param msg Message containing rebalance request params.
     */
    private void onDemandMessage0(UUID nodeId, int topicId, GridPartitionsCopyDemandMessage msg, byte plc) {
        if (msg.rebalanceId() < 0) // Demand node requested context cleanup.
            return;

        ClusterNode demanderNode = cctx.discovery().node(nodeId);

        if (demanderNode == null) {
            U.log(log, "Initial demand message rejected (demander node left the cluster) ["
                + "thread=" + topicId + ", nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']');

            return;
        }

        IgniteSocketChannel ch = null;
        CachePartitionUploadFuture uploadFut = null;
        T2<UUID, Integer> ctxId = new T2<>(nodeId, topicId);

        try {
            synchronized (uploadFutMap) {
                uploadFut = uploadFutMap.getOrDefault(ctxId,
                    new CachePartitionUploadFuture(msg.rebalanceId(), msg.topologyVersion(), msg.assignments()));

                if (uploadFut.rebalanceId < msg.rebalanceId()) {
                    if (!uploadFut.isDone())
                        uploadFut.cancel();

                    uploadFutMap.put(ctxId,
                        uploadFut = new CachePartitionUploadFuture(msg.rebalanceId(),
                            msg.topologyVersion(),
                            msg.assignments()));
                }
            }

            // Need to start new partition upload routine.
            ch = cctx.gridIO().channelToCustomTopic(nodeId, rebalanceThreadTopic(topicId), null, plc);

            backupMgr.backup(uploadFut.rebalanceId,
                uploadFut.getAssigns(),
                new SocketBackupProcessTask(new FileTransferManager<>(cctx.kernalContext(),
                    ch.channel(),
                    dfltIoFactory)),
                uploadFut);

            uploadFut.onDone(true);
        }
        catch (Exception e) {
            U.error(log, "An error occured while processing initial demand request ["
                + "thread=" + topicId + ", nodeId=" + nodeId + ", topVer=" + msg.topologyVersion() + ']', e);

            if (uploadFut != null)
                uploadFut.onDone(e);
        }
        finally {
            U.closeQuiet(ch);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCachePartitionUploadManager.class, this);
    }

    /** */
    private static class SocketBackupProcessTask implements BackupProcessTask {
        /** */
        private final FileTransferManager<PartitionFileMetaInfo> uploader;

        /**
         * @param uploader An upload helper class.
         */
        public SocketBackupProcessTask(FileTransferManager<PartitionFileMetaInfo> uploader) {
            this.uploader = uploader;
        }

        /** {@inheritDoc} */
        @Override public void handlePartition(
            GroupPartitionId grpPartId,
            File file,
            long size
        ) throws IgniteCheckedException {
            uploader.writeFileMetaInfo(new PartitionFileMetaInfo(grpPartId.getGroupId(), file.getName(), size, 0));

            uploader.writeFile(file, 0, size);
        }

        /** {@inheritDoc} */
        @Override public void handleDelta(
            GroupPartitionId grpPartId,
            File file,
            long offset,
            long size
        ) throws IgniteCheckedException {
            uploader.writeFileMetaInfo(new PartitionFileMetaInfo(grpPartId.getGroupId(), file.getName(), size, 1));

            uploader.writeFile(file, offset, size);
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

                Set<Integer> parts = result.putIfAbsent(grpPartsEntry.getKey(), new HashSet<>());

                while (iterator.hasNext())
                    parts.add(iterator.next());
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
