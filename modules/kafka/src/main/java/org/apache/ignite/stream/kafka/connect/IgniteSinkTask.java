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

package org.apache.ignite.stream.kafka.connect;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.Ignition;
import org.apache.ignite.stream.StreamSingleTupleExtractor;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task to consume sequences of SinkRecords and write data to grid.
 */
public class IgniteSinkTask extends SinkTask {
    /** Logger. */
    private static final Logger log = LoggerFactory.getLogger(IgniteSinkTask.class);

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /** Ignite grid configuration file. */
    private static String igniteConfigFile;

    /** Cache name. */
    private static String cacheName;

    /** Entry transformer. */
    private static StreamSingleTupleExtractor<SinkRecord, Object, Object> extractor;

    /** {@inheritDoc} */
    @Override public String version() {
        return new IgniteSinkConnector().version();
    }

    /**
     * Initializes grid client from configPath.
     *
     * @param props Task properties.
     */
    @Override public void start(Map<String, String> props) {
        // Each task has the same parameters -- avoid setting more than once.
        if (cacheName != null)
            return;

        cacheName = props.get(IgniteSinkConstants.CACHE_NAME);
        igniteConfigFile = props.get(IgniteSinkConstants.CACHE_CFG_PATH);

        if (props.containsKey(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE))
            StreamerContext.getStreamer().allowOverwrite(
                Boolean.parseBoolean(props.get(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE)));

        if (props.containsKey(IgniteSinkConstants.CACHE_PER_NODE_DATA_SIZE))
            StreamerContext.getStreamer().perNodeBufferSize(
                Integer.parseInt(props.get(IgniteSinkConstants.CACHE_PER_NODE_DATA_SIZE)));

        if (props.containsKey(IgniteSinkConstants.CACHE_PER_NODE_PAR_OPS))
            StreamerContext.getStreamer().perNodeParallelOperations(
                Integer.parseInt(props.get(IgniteSinkConstants.CACHE_PER_NODE_PAR_OPS)));

        if (props.containsKey(IgniteSinkConstants.SINGLE_TUPLE_EXTRACTOR_CLASS)) {
            String transformerCls = props.get(IgniteSinkConstants.SINGLE_TUPLE_EXTRACTOR_CLASS);
            if (transformerCls != null && !transformerCls.isEmpty()) {
                try {
                    Class<? extends StreamSingleTupleExtractor> clazz =
                        (Class<? extends StreamSingleTupleExtractor<SinkRecord, Object, Object>>)
                            Class.forName(transformerCls);

                    extractor = clazz.newInstance();
                }
                catch (Exception e) {
                    throw new ConnectException("Failed to instantiate the provided transformer!", e);
                }
            }
        }

        stopped = false;
    }

    /**
     * Buffers records.
     *
     * @param records Records to inject into grid.
     */
    @SuppressWarnings("unchecked")
    @Override public void put(Collection<SinkRecord> records) {
        try {
            for (SinkRecord record : records) {
                // Data is flushed asynchronously when CACHE_PER_NODE_DATA_SIZE is reached.
                if (extractor != null) {
                    Map.Entry<Object, Object> entry = extractor.extract(record);
                    StreamerContext.getStreamer().addData(entry.getKey(), entry.getValue());
                }
                else {
                    if (record.key() != null) {
                        StreamerContext.getStreamer().addData(record.key(), record.value());
                    }
                    else {
                        log.error("Failed to stream a record with null key!");
                    }
                }
            }
        }
        catch (ConnectException e) {
            log.error("Failed adding record", e);

            throw new ConnectException(e);
        }
    }

    /**
     * Pushes buffered data to grid. Flush interval is configured by worker configurations.
     *
     * @param offsets Offset information.
     */
    @Override public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (stopped)
            return;

        StreamerContext.getStreamer().flush();
    }

    /**
     * Stops the grid client.
     */
    @Override public void stop() {
        if (stopped)
            return;

        stopped = true;

        StreamerContext.getIgnite().close();
    }

    /**
     * Used by unit test to avoid restart node and valid state of the <code>stopped</code> flag.
     *
     * @param stopped Stopped flag.
     */
    protected static void setStopped(boolean stopped) {
        IgniteSinkTask.stopped = stopped;

        extractor = null;
    }

    /**
     * Streamer context initializing grid and data streamer instances on demand.
     */
    public static class StreamerContext {
        /** Constructor. */
        private StreamerContext() {
        }

        /** Instance holder. */
        private static class Holder {
            private static final Ignite IGNITE = Ignition.start(igniteConfigFile);

            private static final IgniteDataStreamer STREAMER = IGNITE.dataStreamer(cacheName);
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        public static Ignite getIgnite() {
            return Holder.IGNITE;
        }

        /**
         * Obtains data streamer instance.
         *
         * @return Data streamer instance.
         */
        public static IgniteDataStreamer getStreamer() {
            return Holder.STREAMER;
        }
    }
}
