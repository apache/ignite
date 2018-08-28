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

package org.apache.ignite.source.flink;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Apache Flink Ignite source implemented as a RichParallelSourceFunction.
 */
public class IgniteSource extends RichParallelSourceFunction<CacheEvent> {
    /** Serial version uid. */
    private static final long serialVersionUID = 1L;

    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteSource.class);

    /** Default max number of events taken from the buffer at once. */
    private static final int DFLT_EVT_BATCH_SIZE = 1;

    /** Default number of milliseconds timeout for event buffer queue operation. */
    private static final int DFLT_EVT_BUFFER_TIMEOUT = 10;

    /** Event buffer. */
    private BlockingQueue<CacheEvent> evtBuf = new LinkedBlockingQueue<>();

    /** Remote Listener id. */
    private UUID rmtLsnrId;

    /** Flag for stopped state. */
    public transient volatile boolean stopped = false;

    /** Max number of events taken from the buffer at once. */
    private int evtBatchSize = DFLT_EVT_BATCH_SIZE;

    /** Number of milliseconds timeout for event buffer queue operation. */
    private int evtBufTimeout = DFLT_EVT_BUFFER_TIMEOUT;

    /** Local listener. */
    private TaskLocalListener locLsnr = new TaskLocalListener();

    /** Ignite instance. */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** Cache name. */
    private String cacheName;

    /**
     * Sets Ignite instance.
     *
     * @param ignite Ignite instance.
     */
    public void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Sets Event Batch Size.
     *
     * @param evtBatchSize Event Batch Size.
     */
    public void setEvtBatchSize(int evtBatchSize) {
        this.evtBatchSize = evtBatchSize;
    }

    /**
     * Sets Event Buffer timeout.
     *
     * @param evtBufTimeout Event Buffer timeout.
     */
    public void setEvtBufTimeout(int evtBufTimeout) {
        this.evtBufTimeout = evtBufTimeout;
    }

    /** Gets Local Task Listener */
    public TaskLocalListener getLocLsnr() { return this.locLsnr; }

    /**
     * Default IgniteSource constructor.
     *
     * @param cacheName Cache name.
     */
    public IgniteSource(String cacheName) { this.cacheName = cacheName; }

    /**
     * Starts Ignite source.
     * @param filter User defined filter.
     * @param cacheEvts Converts comma-delimited cache events strings to Ignite internal representation.
     *
     * @throws IgniteException If failed.
     */
    @SuppressWarnings("unchecked")
    public void start(IgnitePredicate<CacheEvent> filter, int... cacheEvts) throws Exception {
        A.notNull(cacheName, "Cache name");

        stopped = false;

        TaskRemoteFilter rmtLsnr = new TaskRemoteFilter(cacheName, filter);

        try {
            rmtLsnrId = ignite.events(ignite.cluster().forCacheNodes(cacheName))
                 .remoteListen(locLsnr, rmtLsnr, cacheEvts);
        }
        catch (IgniteException e) {
            log.error("Failed to register event listener!", e);

            throw e;
        }
    }

    /**
     * Transfers data from grid.
     *
     * @param ctx SourceContext.
     */
    @Override public void run(SourceContext<CacheEvent> ctx) {
        List<CacheEvent> evts = new ArrayList<>(evtBatchSize);

        if (stopped)
            return;

        try{
            while (!stopped) {
                if (evtBuf.drainTo(evts, evtBatchSize) > 0) {
                    synchronized (ctx.getCheckpointLock()) {
                        for (CacheEvent evt : evts)
                            ctx.collect(evt);

                        evts = new ArrayList<>(evtBatchSize);
                    }
                }
            }
        } catch (Exception e){
            log.error("Error while processing cache event of " + cacheName, e);
        }
    }

    /** {@inheritDoc} */
    @Override public void cancel() {
        if (stopped)
            return;

        stopped = true;

        if (rmtLsnrId != null && ignite != null) {
            ignite.events(ignite.cluster().forCacheNodes(cacheName))
                .stopRemoteListen(rmtLsnrId);

            rmtLsnrId = null;
        }
    }

    /**
     * Local listener buffering cache events to be further sent to Flink.
     */
    public class TaskLocalListener implements IgniteBiPredicate<UUID, CacheEvent> {
        /** {@inheritDoc} */
        @Override public boolean apply(UUID id, CacheEvent evt) {
            try {
                if (!evtBuf.offer(evt, evtBufTimeout, TimeUnit.MILLISECONDS))
                    log.error("Failed to buffer event {}", evt.name());
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }

            return true;
        }
    }
}

