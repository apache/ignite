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

package org.apache.ignite.stream.flume;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.FlumeException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flume custom sink for Apache Ignite.
 */
public class IgniteSink extends AbstractSink implements Configurable {
    private static final Logger log = LoggerFactory.getLogger(IgniteSink.class);

    /** Ignite configuration file. */
    private String springCfgPath;

    /** Cache name. */
    private String cacheName;

    /** Event transformer implementation class. */
    private String tupleExtractorCls;

    /** Flag to enable overwriting existing values in cache. See {@link IgniteDataStreamer}. */
    private boolean streamerAllowOverwrite;

    /** Flush frequency. See {@link IgniteDataStreamer}. See {@link IgniteDataStreamer}. */
    private long streamerFlushFreq;

    /** Flag to disable write-through behavior. See {@link IgniteDataStreamer}. */
    private boolean skipStore;

    /** Size of per node key-value pairs buffer. See {@link IgniteDataStreamer}. */
    private int perNodeDataSize;

    /** Maximum number of parallel stream operations for a single node. See {@link IgniteDataStreamer}. */
    private int perNodeParallelOps;

    /** Flume streamer. */
    private FlumeStreamer<Event, Object, Object> flumeStreamer;

    /** Ignite instance. */
    private Ignite ignite;

    /** Empty constructor. */
    public IgniteSink() {
    }

    /**
     * Constructor with provided grid instance.
     *
     * @param ignite Grid instance.
     */
    public IgniteSink(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Returns cache name for the sink.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Sink configurations with Ignite-specific settings.
     *
     * @param context Context for sink.
     */
    @Override public void configure(Context context) {
        springCfgPath = context.getString(IgniteSinkConstants.CFG_PATH);
        cacheName = context.getString(IgniteSinkConstants.CFG_CACHE_NAME);
        tupleExtractorCls = context.getString(IgniteSinkConstants.CFG_TUPLE_EXTRACTOR);
        streamerAllowOverwrite = context.getBoolean(IgniteSinkConstants.CFG_STREAMER_OVERWRITE, false);
        streamerFlushFreq = context.getLong(IgniteSinkConstants.CFG_STREAMER_FLUSH_FREQ, 0L);
        skipStore = context.getBoolean(IgniteSinkConstants.CFG_STREAMER_SKIP_STORE, false);
        perNodeDataSize = context.getInteger(IgniteSinkConstants.CFG_STREAMER_NODE_BUFFER_SIZE, IgniteDataStreamer.DFLT_PER_NODE_BUFFER_SIZE);
        perNodeParallelOps = context.getInteger(IgniteSinkConstants.CFG_STREAMER_NODE_PARALLEL_OPS, IgniteDataStreamer.DFLT_MAX_PARALLEL_OPS);
    }

    /**
     * Starts a grid and a streamer.
     */
    @Override synchronized public void start() {
        A.notNull(tupleExtractorCls, "Tuple Extractor class");
        A.notNull(springCfgPath, "Ignite config file");

        try {
            if (ignite == null)
                ignite = Ignition.start(springCfgPath);

            ignite.getOrCreateCache(cacheName);

            if (tupleExtractorCls != null && !tupleExtractorCls.isEmpty()) {
                Class<? extends StreamMultipleTupleExtractor> clazz =
                    (Class<? extends StreamMultipleTupleExtractor>)Class.forName(tupleExtractorCls);
                StreamMultipleTupleExtractor<Event, Object, Object> tupleExtractor = clazz.newInstance();
                flumeStreamer = new FlumeStreamer(ignite.dataStreamer(cacheName), tupleExtractor);
                flumeStreamer.getStreamer().allowOverwrite(streamerAllowOverwrite);
                flumeStreamer.getStreamer().autoFlushFrequency(streamerFlushFreq);
                flumeStreamer.getStreamer().skipStore(skipStore);
                flumeStreamer.getStreamer().perNodeBufferSize(perNodeDataSize);
                flumeStreamer.getStreamer().perNodeParallelOperations(perNodeParallelOps);
            }
        }
        catch (Exception e) {
            log.error("Failed while starting an Ignite streamer", e);
            throw new FlumeException("Failed while starting an Ignite streamer", e);
        }

        super.start();
    }

    /**
     * Stops the streamer and the grid.
     */
    @Override synchronized public void stop() {
        if (flumeStreamer != null)
            flumeStreamer.getStreamer().close();

        if (ignite != null)
            ignite.close();

        super.stop();
    }

    /**
     * Processes Flume events from the sink.
     */
    @Override public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event;

        try {
            transaction.begin();
            event = channel.take();

            if (event != null)
                flumeStreamer.writeEvent(event);
            else
                status = Status.BACKOFF;

            transaction.commit();
        }
        catch (Exception e) {
            log.error("Failed to process events", e);
            transaction.rollback();
            throw new EventDeliveryException(e);
        }
        finally {
            transaction.close();
        }

        return status;
    }
}
