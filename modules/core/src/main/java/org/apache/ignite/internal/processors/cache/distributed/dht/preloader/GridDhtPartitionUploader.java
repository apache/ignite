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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.backup.BackupProcessTask;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIoUploader;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.partstate.GroupPartitionId;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.getPartitionFile;

/** */
public class GridDhtPartitionUploader {
    /** */
    private final CacheGroupContext grp;

    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** Cache working directory. */
    private final File grpDir;

    /** */
    private final IgniteLogger log;

    /** */
    private final Map<T3<UUID, Integer, AffinityTopologyVersion>, GridDhtPartitionUploader.UploadContext> uploadCtx = new HashMap<>();

    /** Factory to provide I/O interfaces for read/write operations with files */
    private final FileIOFactory ioFactory;

    /** */
    private GridDhtPartitionTopology top;

    /** */
    private volatile PartitionUploadFuture partUploader;

    /** */
    GridDhtPartitionUploader(CacheGroupContext grp) {
        assert grp != null;
        assert grp.persistenceEnabled();

        this.grp = grp;
        cctx = grp.shared();
        grpDir = cacheWorkDir(((GridCacheDatabaseSharedManager)grp.shared().database())
                .getFileStoreManager()
                .workDir(),
            grp.config());
        log = cctx.logger(getClass());
        top = grp.topology();
        ioFactory = new RandomAccessFileIOFactory();
        partUploader = new PartitionUploadFuture(log);

        // Default implementation.
        partUploader.onDone(true);
    }

    /** */
    public void stop() {
        partUploader.cancel();
    }

    /** */
    public void onTopologyChanged() {

    }

    /** */
    public void handleDemandMessage(int topicId, UUID nodeId, GridDhtPartitionDemandMessage demandMsg) {
        assert demandMsg != null;
        assert nodeId != null;

        if (demandMsg.rebalanceId() < 0) // Demand node requested context cleanup.
            return;

        // Todo Check is demander node supports file transfer?

        ClusterNode demanderNode = grp.shared().discovery().node(nodeId);

        if (demanderNode == null) {
            U.log(log, "Initial demand message rejected (demander left the cluster) ["
                + "grp=" + grp.cacheOrGroupName() + ", nodeId=" + nodeId + ", topVer=" + demandMsg.topologyVersion() +
                ", topic=" + topicId + "]");

            return;
        }

        // Demand request should not contain empty partitions for initial message processing.
        if (demandMsg.partitions() == null || demandMsg.partitions().isEmpty()) {
            U.log(log, "Empty demand message (no context and partitions) ["
                + "grp=" + grp.cacheOrGroupName() + ", nodeId=" + nodeId +
                ", topVer=" + demandMsg.topologyVersion() + ", topic=" + topicId + "]");

            return;
        }

        // Need to handle only initial demand message request. The others should be ignored.
        T3<UUID, Integer, AffinityTopologyVersion> ctxId = new T3<>(nodeId, topicId, demandMsg.topologyVersion());

        UploadContext upCtx = null;
        IgniteSocketChannel ch = null;

        try {
            synchronized (uploadCtx) {
                upCtx = uploadCtx.get(ctxId);

                if (upCtx == null) {
                    U.log(log, "Starting supplying rebalancing ["
                        + "grp=" + grp.cacheOrGroupName() + ", nodeId=" + nodeId +
                        ", topVer=" + demandMsg.topologyVersion() + ", topic=" + topicId +
                        ", fullPartitions=" + S.compact(demandMsg.partitions().fullSet()) +
                        ", histPartitions=" + S.compact(demandMsg.partitions().historicalSet()) + "]");

                    uploadCtx.put(ctxId, upCtx = new UploadContext(demandMsg.partitions().fullSet(), demandMsg.rebalanceId()));
                }
                else {
                    if (demandMsg.rebalanceId() < upCtx.rebalanceId) {
                        // Stale message, return context back and return.
                        uploadCtx.put(ctxId, upCtx);

                        return;
                    }
                }
            }

            if (!partUploader.isDone())
                partUploader.cancel();

            // Need to start new partition upload routine.
            ch = cctx.io().channelToCustomTopic(nodeId, demandMsg.topic(), demandMsg, PUBLIC_POOL);

            partUploader = new PartitionUploadFuture(ch, ctxId, upCtx, log);

            // Loop to read partition files.

            // File cfgFile - partition config
            // Channel -
            // Future -
            // Checkpointed file - ?
            // CRC-32 calculation?
            FileIoUploader uploader = new FileIoUploader(ch.channel(), ioFactory, log);

            U.log(log, "Start uploading cache group partition procedure [grp=" + grp.cacheOrGroupName() +
                ", channel=" + ch + ", upCtx=" + upCtx + ']');

            for (Integer partId : upCtx.parts) {
                File partFile = getPartitionFile(grpDir, partId);

                uploader.upload(partFile);

                U.log(log, "Partition file uploaded: " + partFile.getPath());
            }

            uploader.close();
        }
        catch (Exception e) {
            if (upCtx != null)
                uploadCtx.remove(ctxId);

            U.error(log, "An error occured while processing initial demand request ["
                + "grp=" + grp.cacheOrGroupName() + ", nodeId=" + nodeId + ", topVer=" + demandMsg.topologyVersion() +
                ", topic=" + topicId + "]", e);
        }
        finally {
            U.closeQuiet(ch);
        }
    }

    /** */
    private static class FileBackupTask implements BackupProcessTask {
        /** */
        private final FileIoUploader uploader;

        /** */
        public FileBackupTask(FileIoUploader uploader) {
            this.uploader = uploader;
        }

        /** {@inheritDoc} */
        @Override public void handlePartition(
            GroupPartitionId grpPartId,
            File file,
            long offset,
            long count
        ) throws IgniteCheckedException {
            uploader.upload(file);
        }

        /** {@inheritDoc} */
        @Override public void handleDelta(
            GroupPartitionId grpPartId,
            File file,
            long offset,
            long size
        ) throws IgniteCheckedException {
            uploader.upload(file);
        }
    }

    /** */
    private static class UploadContext {
        /** Set of partitions which weren't uploaded yet to the demander node. */
        private final Set<Integer> parts;

        /** */
        private final long rebalanceId;

        /**
         * @param parts Set of partitions which weren't sent yet.
         * @param rebalanceId Rebalance id.
         */
        UploadContext(Set<Integer> parts, long rebalanceId) {
            this.parts = new HashSet<>(parts);
            this.rebalanceId = rebalanceId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "UploadContext{" +
                "parts=" + parts +
                ", rebalanceId=" + rebalanceId +
                '}';
        }
    }

    /** */
    private class PartitionUploadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final IgniteSocketChannel channel;

        /** */
        private final T3<UUID, Integer, AffinityTopologyVersion> ctxId;

        /** */
        private final UploadContext uploadCtx;

        /** */
        private final IgniteLogger log;

        /** */
        public PartitionUploadFuture(IgniteLogger log) {
            this(null, null, null, log);
        }

        /** */
        public PartitionUploadFuture(
            IgniteSocketChannel channel,
            T3<UUID, Integer, AffinityTopologyVersion> ctxId,
            UploadContext uploadCtx,
            IgniteLogger log
        ) {
            this.channel = channel;
            this.ctxId = ctxId;
            this.uploadCtx = uploadCtx;
            this.log = log;
        }

        /** */
        @Override public boolean cancel() {
            synchronized (this) {
                if (isDone())
                    return true;

                U.log(log, "Upload cancelled [grp=" + grp.cacheOrGroupName() + ", nodeId=" + ctxId.get1() +
                    ", topVer=" + ctxId.get3() + "]");

                U.closeQuiet(channel);
            }

            return true;
        }
    }
}
