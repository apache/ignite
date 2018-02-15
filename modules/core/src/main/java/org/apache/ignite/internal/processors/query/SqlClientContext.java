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
import java.util.Map;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Container for connection properties passed by various drivers (JDBC drivers, possibly ODBC) having notion of an
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

    /** Lazy query execution flag. */
    private final boolean lazy;

    /** Skip reducer on update flag. */
    private final boolean skipReducerOnUpdate;

    /** Allow overwrites for duplicate keys on streamed {@code INSERT}s. */
    private final boolean streamAllowOverwrite;

    /** Parallel ops count per node for data streamer. */
    private final int streamNodeParOps;

    /** Node buffer size for data streamer. */
    private final int streamNodeBufSize;

    /** Auto flush frequency for streaming. */
    private final long streamFlushTimeout;

    /** Streamers for various caches. */
    private final Map<String, IgniteDataStreamer<?, ?>> streamers;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Kernal context.
     * @param distributedJoins Distributed joins flag.
     * @param enforceJoinOrder Enforce join order flag.
     * @param collocated Collocated flag.
     * @param lazy Lazy query execution flag.
     * @param skipReducerOnUpdate Skip reducer on update flag.
     * @param stream Streaming state flag
     * @param streamAllowOverwrite Allow overwrites for duplicate keys on streamed {@code INSERT}s.
     * @param streamNodeParOps Parallel ops count per node for data streamer.
     * @param streamNodeBufSize Node buffer size for data streamer.
     * @param streamFlushTimeout Auto flush frequency for streaming.
     */
    public SqlClientContext(GridKernalContext ctx, boolean distributedJoins, boolean enforceJoinOrder,
        boolean collocated, boolean lazy, boolean skipReducerOnUpdate, boolean stream, boolean streamAllowOverwrite,
        int streamNodeParOps, int streamNodeBufSize, long streamFlushTimeout) {
        this.ctx = ctx;
        this.distributedJoins = distributedJoins;
        this.enforceJoinOrder = enforceJoinOrder;
        this.collocated = collocated;
        this.lazy = lazy;
        this.skipReducerOnUpdate = skipReducerOnUpdate;
        this.streamAllowOverwrite = streamAllowOverwrite;
        this.streamNodeParOps = streamNodeParOps;
        this.streamNodeBufSize = streamNodeBufSize;
        this.streamFlushTimeout = streamFlushTimeout;

        streamers = stream ? new HashMap<>() : null;

        log = ctx.log(SqlClientContext.class.getName());

        ctx.query().registerClientContext(this);
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
        return streamers != null;
    }

    /**
     * @param cacheName Cache name.
     * @return Streamer for given cache.
     */
    public IgniteDataStreamer<?, ?> streamerForCache(String cacheName) {
        Map<String, IgniteDataStreamer<?, ?>> curStreamers = streamers;

        if (curStreamers == null)
            return null;

        IgniteDataStreamer<?, ?> res = curStreamers.get(cacheName);

        if (res != null)
            return res;

        res = ctx.grid().dataStreamer(cacheName);

        IgniteDataStreamer<?, ?> exStreamer = curStreamers.putIfAbsent(cacheName, res);

        if (exStreamer == null) {
            res.autoFlushFrequency(streamFlushTimeout);

            res.allowOverwrite(streamAllowOverwrite);

            if (streamNodeBufSize > 0)
                res.perNodeBufferSize(streamNodeBufSize);

            if (streamNodeParOps > 0)
                res.perNodeParallelOperations(streamNodeParOps);

            return res;
        }
        else { // Someone got ahead of us.
            res.close();

            return exStreamer;
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        ctx.query().unregisterClientContext(this);

        if (streamers == null)
            return;

        for (IgniteDataStreamer<?, ?> s : streamers.values())
            U.close(s, log);
    }
}
