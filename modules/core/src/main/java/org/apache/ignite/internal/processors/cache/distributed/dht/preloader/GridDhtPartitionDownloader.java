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
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIODownloader;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.cacheWorkDir;

/**
 *
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

    /**
     * @param lastFut Last future to set.
     */
    public void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {

    }

    /** */
    public void handleChannelCreated(IgniteSocketChannel channel) {
        if (log.isInfoEnabled())
            log.info("Handle channel creation event with: " + channel);

        assert channel != null;

        if (channel.groupId() != grp.groupId())
            throw new IgniteException("Incorrect processing [expected=" + grp.groupId() +
                ", actual=" + channel.groupId() + ']');

        FileIODownloader downloader = new FileIODownloader((ReadableByteChannel)channel.channel(), ioFactory, grpDir, log);

        try {
            File partFile = downloader.download();

            if (log.isInfoEnabled())
                log.info("Partition file downloaded: " + partFile.getPath());
        }
        catch (IgniteCheckedException e) {
            log.error("Download process terminated unexpectedly.", e);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDownloader.class, this);
    }

    /** */
    private static class PartitionDonwloadFuture extends GridFutureAdapter<Boolean> {

    }
}
