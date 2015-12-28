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

    /** Ignite instance. */
    private static Ignite ignite;

    /** Data streamer. */
    private static IgniteDataStreamer dataStreamer;

    /** {@inheritDoc} */
    @Override public String version() {
        return new IgniteSinkConnector().version();
    }

    /**
     * Initializes grid client from configPath.
     *
     * @param props Task properties.
     */
    private static synchronized void initializeIgnite(Map<String, String> props) {
        if (ignite == null) {
            ignite = Ignition.start(props.get(IgniteSinkConstants.CACHE_CFG_PATH));

            dataStreamer = ignite.dataStreamer(props.get(IgniteSinkConstants.CACHE_NAME));

            if (props.containsKey(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE))
                dataStreamer.allowOverwrite(Boolean.parseBoolean(props.get(IgniteSinkConstants.CACHE_ALLOW_OVERWRITE)));
            if (props.containsKey(IgniteSinkConstants.CACHE_PER_NODE_DATA_SIZE))
                dataStreamer.perNodeBufferSize(Integer.parseInt(props.get(IgniteSinkConstants.CACHE_PER_NODE_DATA_SIZE)));
            if (props.containsKey(IgniteSinkConstants.CACHE_PER_NODE_PAR_OPS))
                dataStreamer.perNodeParallelOperations(Integer.parseInt(props.get(IgniteSinkConstants.CACHE_PER_NODE_PAR_OPS)));
        }
    }

    /**
     * Finalizes grid client.
     */
    private static synchronized void finalizeIgnite() {
        if (ignite != null) {
            dataStreamer.close();
            dataStreamer = null;

            ignite.close();
            ignite = null;
        }
    }

    /**
     * Creates cache.
     *
     * @param props Properties.
     */
    @Override public void start(Map<String, String> props) {
        initializeIgnite(props);
    }

    /**
     * Buffers records.
     *
     * @param records Records to inject into grid.
     */
    @Override public void put(Collection<SinkRecord> records) {
        try {
            for (SinkRecord record : records) {
                if (record.key() != null) {
                    // Data is flushed asynchronously when CACHE_PER_NODE_DATA_SIZE is reached
                    dataStreamer.addData(record.key(), record.value());
                }
                else {
                    log.error("Failed to stream a record with null key!");
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
        if (dataStreamer != null)
            dataStreamer.flush();
    }

    /**
     * Stops the grid client.
     */
    @Override public void stop() {
        finalizeIgnite();
    }
}
