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

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.cache.CacheGroupContext;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.nio.channel.IgniteSocketChannel;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 *
 */
public class GridDhtPartitionDownloader {
    /** */
    private final GridCacheSharedContext<?, ?> cctx;

    /** */
    private final CacheGroupContext grp;

    /** */
    private final IgniteLogger log;

    /** Cached rebalance topics. */
    private final Map<Integer, Object> rebalanceTopics;

    /** Rebalance future. */
    @GridToStringInclude
    private volatile PartitionDonwloadFuture downloadFut;

    /** Last exchange future. */
    private volatile GridDhtPartitionsExchangeFuture lastExchangeFut;

    /**
     * @param grp Ccahe group.
     */
    public GridDhtPartitionDownloader(CacheGroupContext grp) {
        assert grp != null;

        this.grp = grp;
        cctx = grp.shared();
        log = cctx.logger(getClass());
        downloadFut = new PartitionDonwloadFuture(); //Dummy.

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

        lastExchangeFut = null;
    }

    /**
     * Sets last exchange future.
     *
     * @param lastFut Last future to set.
     */
    void onTopologyChanged(GridDhtPartitionsExchangeFuture lastFut) {
        lastExchangeFut = lastFut;
    }

    /** */
    public void handleChannelCreated(IgniteSocketChannel channel) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtPartitionDownloader.class, this);
    }

    /** */
    private class PartitionDonwloadFuture extends GridFutureAdapter<Boolean> {

    }
}
