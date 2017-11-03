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

package org.apache.ignite.sink.flink;

import java.util.Map;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.internal.util.typedef.internal.A;

/**
 * Apache Flink Ignite sink implemented as a RichSinkFunction.
 */
public class IgniteSink<IN> extends RichSinkFunction<IN> {
    /** Default flush frequency. */
    private static final long DFLT_FLUSH_FREQ = 10000L;

    /** Logger. */
    private final transient IgniteLogger log;

    /** Automatic flush frequency. */
    private long autoFlushFrequency = DFLT_FLUSH_FREQ;

    /** Enables overwriting existing values in cache. */
    private boolean allowOverwrite = false;

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /** Ignite grid configuration file. */
    private static String igniteCfgFile;

    /** Cache name. */
    private static String cacheName;

    /**
     * Gets the cache name.
     *
     * @return Cache name.
     */
    public String getCacheName() {
        return cacheName;
    }

    /**
     * Gets Ignite configuration file.
     *
     * @return Configuration file.
     */
    public String getIgniteConfigFile() {
        return igniteCfgFile;
    }

    /**
     * Obtains data flush frequency.
     *
     * @return Flush frequency.
     */
    public long getAutoFlushFrequency() {
        return autoFlushFrequency;
    }

    /**
     * Specifies data flush frequency into the grid.
     *
     * @param autoFlushFrequency Flush frequency.
     */
    public void setAutoFlushFrequency(long autoFlushFrequency) {
        this.autoFlushFrequency = autoFlushFrequency;
    }

    /**
     * Obtains flag for enabling overwriting existing values in cache.
     *
     * @return True if overwriting is allowed, false otherwise.
     */
    public boolean getAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * Enables overwriting existing values in cache.
     *
     * @param allowOverwrite Flag value.
     */
    public void setAllowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;
    }

    /**
     * Default IgniteSink constructor.
     *
     * @param cacheName Cache name.
     * @param igniteCfgFile Ignite configuration file.
     */
    public IgniteSink(String cacheName, String igniteCfgFile) {
        this.cacheName = cacheName;
        this.igniteCfgFile = igniteCfgFile;
        this.log = SinkContext.getIgnite().log();
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    @SuppressWarnings("unchecked")
    public void start() throws IgniteException {
        A.notNull(igniteCfgFile, "Ignite config file");
        A.notNull(cacheName, "Cache name");

        SinkContext.getStreamer().autoFlushFrequency(autoFlushFrequency);
        SinkContext.getStreamer().allowOverwrite(allowOverwrite);

        stopped = false;
    }

    /**
     * Stops streamer.
     *
     * @throws IgniteException If failed.
     */
    public void stop() throws IgniteException {
        if (stopped)
            return;

        stopped = true;

        SinkContext.getStreamer().close();
        SinkContext.getIgnite().cache(cacheName).close();
        SinkContext.getIgnite().close();
    }

    /**
     * Transfers data into grid. It is called when new data
     * arrives to the sink, and forwards it to {@link IgniteDataStreamer}.
     *
     * @param in IN.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void invoke(IN in) {
        try {
            if (!(in instanceof Map))
                throw new IgniteException("Map as a streamer input is expected!");

            SinkContext.getStreamer().addData((Map)in);
        }
        catch (Exception e) {
            log.error("Error while processing IN of " + cacheName, e);
        }
    }

    /**
     * Streamer context initializing grid and data streamer instances on demand.
     */
    private static class SinkContext {
        /** Constructor. */
        private SinkContext() {
        }

        /** Instance holder. */
        private static class Holder {
            private static final Ignite IGNITE = Ignition.start(igniteCfgFile);
            private static final IgniteDataStreamer STREAMER = IGNITE.dataStreamer(cacheName);
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        private static Ignite getIgnite() {
            return Holder.IGNITE;
        }

        /**
         * Obtains data streamer instance.
         *
         * @return Data streamer instance.
         */
        private static IgniteDataStreamer getStreamer() {
            return Holder.STREAMER;
        }
    }
}
