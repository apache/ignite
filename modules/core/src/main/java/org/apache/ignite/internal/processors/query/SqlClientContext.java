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

package org.apache.ignite.internal.processors.query;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import javax.cache.configuration.Factory;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.thread.IgniteThread;

/**
 * Container for connection properties passed by various drivers (JDBC, ODBC drivers) having notion of an
 * <b>SQL connection</b> - Ignite basically does not have one.<p>
 * Also contains anything that a driver may need to share between threads processing queries of logically same client -
 * see JDBC thin driver
 */
public class SqlClientContext implements AutoCloseable {
    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Distributed joins flag. */
    private final boolean distributedJoins;

    /** Enforce join order flag. */
    private final boolean enforceJoinOrder;

    /** Collocated flag. */
    private final boolean collocated;

    /** Replicated caches only flag. */
    private final boolean replicatedOnly;

    /** Lazy query execution flag. */
    private final boolean lazy;

    /** Skip reducer on update flag. */
    private final boolean skipReducerOnUpdate;

    /** Monitor. */
    private final Object mux = new Object();

    /** Allow overwrites for duplicate keys on streamed {@code INSERT}s. */
    private boolean streamAllowOverwrite;

    /** Parallel ops count per node for data streamer. */
    private int streamNodeParOps;

    /** Node buffer size for data streamer. */
    private int streamNodeBufSize;

    /** Auto flush frequency for streaming. */
    private long streamFlushTimeout;

    /** Stream ordered. */
    private boolean streamOrdered;

    /** Streamers for various caches. */
    private volatile Map<String, IgniteDataStreamer<?, ?>> streamers;

    /** Ordered batch thread. */
    private IgniteThread orderedBatchThread;

    /** Ordered batch worker factory. */
    private Factory<GridWorker> orderedBatchWorkerFactory;

    /** Count of the processed ordered batch requests. Used to wait end of processing all request before starts
     * the processing the last request. */
    private long totalProcessedOrderedReqs;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Kernal context.
     * @param orderedBatchWorkerFactory Ordered batch worker factory.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param collocated Collocated flag.
     * @param replicatedOnly Replicated caches only flag.
     * @param lazy Lazy query execution flag.
     * @param skipReducerOnUpdate Skip reducer on update flag.
     */
    public SqlClientContext(GridKernalContext ctx, Factory<GridWorker> orderedBatchWorkerFactory,
        boolean distributedJoins, boolean enforceJoinOrder,
        boolean collocated, boolean replicatedOnly, boolean lazy, boolean skipReducerOnUpdate) {
        this.ctx = ctx;
        this.orderedBatchWorkerFactory = orderedBatchWorkerFactory;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.collocated = collocated;
        this.replicatedOnly = replicatedOnly;
        this.lazy = lazy;
        this.skipReducerOnUpdate = skipReducerOnUpdate;

        log = ctx.log(SqlClientContext.class.getName());
    }

    /**
     * Turn on streaming on this client context.
     *
     * @param allowOverwrite Whether streaming should overwrite existing values.
     * @param flushFreq Flush frequency for streamers.
     * @param perNodeBufSize Per node streaming buffer size.
     * @param perNodeParOps Per node streaming parallel operations number.
     * @param ordered Ordered stream flag.
     */
    public void enableStreaming(boolean allowOverwrite, long flushFreq, int perNodeBufSize,
        int perNodeParOps, boolean ordered) {
        synchronized (mux) {
            if (isStream())
                return;

            streamers = new HashMap<>();

            this.streamAllowOverwrite = allowOverwrite;
            this.streamFlushTimeout = flushFreq;
            this.streamNodeBufSize = perNodeBufSize;
            this.streamNodeParOps = perNodeParOps;
            this.streamOrdered = ordered;

            if (ordered) {
                orderedBatchThread = new IgniteThread(orderedBatchWorkerFactory.create());

                orderedBatchThread.start();
            }
        }
    }

    /**
     * Turn off streaming on this client context - with closing all open streamers, if any.
     */
    public void disableStreaming() {
        synchronized (mux) {
            if (!isStream())
                return;

            Iterator<IgniteDataStreamer<?, ?>> it = streamers.values().iterator();

            while (it.hasNext()) {
                IgniteDataStreamer<?, ?> streamer = it.next();

                U.close(streamer, log);

                it.remove();
            }

            streamers = null;

            orderedBatchThread = null;
        }
    }

    /**
     * @return Collocated flag.
     */
    public boolean isCollocated() {
        return collocated;
    }

    /**
     * @return Distributed joins flag.
     */
    public boolean isDistributedJoins() {
        return distributedJoins;
    }

    /**
     * @return Enforce join order flag.
     */
    public boolean isEnforceJoinOrder() {
        return enforceJoinOrder;
    }

    /**
     * @return Replicated caches only flag.
     */
    public boolean isReplicatedOnly() {
        return replicatedOnly;
    }

    /**
     * @return Lazy query execution flag.
     */
    public boolean isLazy() {
        return lazy;
    }

    /**
     * @return Skip reducer on update flag,
     */
    public boolean isSkipReducerOnUpdate() {
        return skipReducerOnUpdate;
    }

    /**
     * @return Streaming state flag (on or off).
     */
    public boolean isStream() {
        synchronized (mux) {
            return streamers != null;
        }
    }

    /**
     * @return Stream ordered flag.
     */
    public boolean isStreamOrdered() {
        synchronized (mux) {
            return streamOrdered;
        }
    }

    /**
     * @param cacheName Cache name.
     * @return Streamer for given cache.
     */
    public IgniteDataStreamer<?, ?> streamerForCache(String cacheName) {
        synchronized (mux) {
            if (streamers == null)
                return null;

            IgniteDataStreamer<?, ?> res = streamers.get(cacheName);

            if (res != null)
                return res;

            res = ctx.grid().dataStreamer(cacheName);

            res.autoFlushFrequency(streamFlushTimeout);

            res.allowOverwrite(streamAllowOverwrite);

            if (streamNodeBufSize > 0)
                res.perNodeBufferSize(streamNodeBufSize);

            if (streamNodeParOps > 0)
                res.perNodeParallelOperations(streamNodeParOps);

            streamers.put(cacheName, res);

            return res;
        }
    }

    /**
     * Waits when total processed ordered requests count  to be equal to specified value.
     * @param total Expected total processed request.
     */
    public void waitTotalProcessedOrderedRequests(long total) {
        synchronized (mux) {
            while (totalProcessedOrderedReqs < total) {
                try {
                    mux.wait();
                }
                catch (InterruptedException e) {
                    throw new IgniteException("Waiting for end of processing the last batch is interrupted", e);
                }
            }
        }
    }

    /**
     *
     */
    public void orderedRequestProcessed() {
        synchronized (mux) {
            totalProcessedOrderedReqs++;

            mux.notify();
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        if (streamers == null)
            return;

        for (IgniteDataStreamer<?, ?> s : streamers.values())
            U.close(s, log);
    }
}
