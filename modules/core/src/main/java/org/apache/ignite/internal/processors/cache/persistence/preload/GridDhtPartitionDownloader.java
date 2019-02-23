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
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPreloaderAssignments;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.FileTransferManager;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.meta.PartitionFileMetaInfo;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;
import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.compact;

/**
 * TODO must be removed
 */
public class GridDhtPartitionDownloader {
    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final CacheGroupContext grp;

    /** Cache working directory. */
    private final File grpDir;

    /** */
    private final IgniteLogger log;

    /** Factory to provide I/O interfaces for read/write operations with files */
    private final FileIOFactory ioFactory;

    /** Cached rebalance topics. */
    private final Map<Integer, Object> rebalanceTopics;

    /** Rebalance future. */
    @GridToStringInclude
    private volatile PartitionDonwloadFuture downloadFut;

    /**
     * @param grp Ccahe group.
     */
    public GridDhtPartitionDownloader(CacheGroupContext grp) {
        assert grp != null;
        assert grp.persistenceEnabled();

        this.grp = grp;
        cctx = grp.shared();
        log = cctx.logger(getClass());
        grpDir = cacheWorkDir(((GridCacheDatabaseSharedManager)grp.shared().database())
                .getFileStoreManager()
                .workDir(),
            grp.config());
        ioFactory = new RandomAccessFileIOFactory();
        downloadFut = new PartitionDonwloadFuture();

        Map<Integer, Object> tops = new HashMap<>();

        for (int idx = 0; idx < grp.shared().kernalContext().config().getRebalanceThreadPoolSize(); idx++)
            tops.put(idx, GridCachePartitionExchangeManager.rebalanceTopic(idx));

        rebalanceTopics = tops;
    }

    /**
     * Start.
     */
    public void start() {
        // No-op.
    }

    /**
     * Stop.
     */
    public void stop() {
        try {
            downloadFut.cancel();
        }
        catch (Exception ignored) {
            downloadFut.onDone(false);
        }
    }

    /** */
    public void addAssignments(GridDhtPreloaderAssignments assignments) {
        try {
            downloadFut.cancel();

            Map<UUID, Set<Integer>> assignsCopy = assignments.entrySet()
                .stream()
                .collect(Collectors.toMap(k -> k.getKey().id(),
                    v -> v.getValue().partitions().fullSet()));

            downloadFut = new PartitionDonwloadFuture(assignsCopy);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Unable to handle new assignments: " + assignments);

            downloadFut.onDone(false);
        }
    }

    /**
     * @param lastFut Last future to set.
     */
    public void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {

    }

    /** */
    public void handleChannelCreated(IgniteSocketChannel channel) {
        assert channel != null;
        assert channel.groupId() == grp.groupId();

        UUID rmtId = channel.id().nodeId();

        final Set<Integer> parts = downloadFut.remaining.get(rmtId);

        if (parts == null || parts.isEmpty())
            return;

        U.log(log, "Handle created channel [grp=" + grp.cacheOrGroupName() +
            ", channel=" + channel + ", grpDir=" + grpDir + ", rmtId=" + rmtId +
            ", parts=" + compact(parts) + ']');

        try {
            File out = new File(grpDir, "downloads");

            Files.createDirectory(out.toPath());

            FileTransferManager<PartitionFileMetaInfo> downloader =
                new FileTransferManager<>(cctx.kernalContext(), channel.channel(), ioFactory);

            for (Integer partId : parts) {
//                downloader.readFile();

                // Check file name. Remove after at final implementation line.
//                String fname = partFile.getName();
//                int pos = fname.lastIndexOf('.');

//                assert pos > 0;

//                fname.substring(0, pos).equals(String.format(PART_FILE_TEMPLATE, partId));

                // Merge procedure should be implemented here.
//
//                if (log.isInfoEnabled())
//                    log.info("Partition file downloaded: " + partFile.getName());
            }

            downloader.close();

            downloadFut.onDone();

            // Todo Merge partition files.
        }
        catch (Exception e) {
            log.error("Download process terminated unexpectedly.", e);

            downloadFut.onDone(e);
        }
        finally {
            U.closeQuiet(channel);
        }
    }

    /** */
    private static class PartitionDonwloadFuture extends GridFutureAdapter<Boolean> {
        /** */
        private final Map<UUID, Set<Integer>> remaining;

        /** */
        public PartitionDonwloadFuture() {
            this(new HashMap<>());
        }

        /** */
        public PartitionDonwloadFuture(Map<UUID, Set<Integer>> remaining) {
            this.remaining = remaining;
        }

        /** {@inheritDoc} */
        @Override public boolean cancel() throws IgniteCheckedException {
            remaining.clear();

            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "PartitionDonwloadFuture{" +
                "remaining=" + remaining +
                '}';
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDownloader.class, this);
    }
}
