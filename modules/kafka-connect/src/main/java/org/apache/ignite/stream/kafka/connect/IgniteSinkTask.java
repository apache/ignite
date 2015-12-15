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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.ignite.Ignite;
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

    /** Cache name. */
    private String cacheName;

    private List<SinkRecord> buffer;

    /** {@inheritDoc} */
    @Override public String version() {
        return new IgniteSinkConnector().version();
    }

    /**
     * Initializes grid client from configPath.
     *
     * @param configPath Ignite Spring config file.
     */
    private static synchronized void initializeIgnite(String configPath) {
        if (ignite == null)
            ignite = Ignition.start(configPath);
    }

    /**
     * Finalizes grid client.
     */
    private static synchronized void finalizeIgnite() {
        if (ignite != null) {
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
        buffer = new LinkedList<>();

        cacheName = props.get(IgniteSinkConstants.CACHE_NAME);

        initializeIgnite(props.get(IgniteSinkConstants.CACHE_CFG_PATH));
    }

    /**
     * Buffers records.
     *
     * @param records Records to inject into grid.
     */
    @Override public void put(Collection<SinkRecord> records) {
        try {
            for (SinkRecord record : records) {
                if (record.key() != null)
                    buffer.add(record);
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
        final Map<Object, Object> recordMap = new HashMap<>();

        for (SinkRecord record : buffer)
            recordMap.put(record.key(), record.value());

        buffer.clear();

        if (ignite != null && recordMap.size() > 0)
            ignite.cache(cacheName).putAll(recordMap);
    }

    /**
     * Stops the grid client.
     */
    @Override public void stop() {
        finalizeIgnite();
    }
}
