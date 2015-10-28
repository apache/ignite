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

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;

/**
 * Flume streamer that receives events from a sink and feeds key-value pairs into an {@link IgniteDataStreamer}.
 */
public abstract class FlumeStreamer<Event, K, V> {
    private IgniteDataStreamer<K, V> dataStreamer;

    public IgniteDataStreamer<K, V> getDataStreamer() {
        return dataStreamer;
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException if failed.
     */
    public void start(Ignite ignite, String cacheName) {
        ignite.getOrCreateCache(cacheName);
        dataStreamer = ignite.dataStreamer(cacheName);
    }

    /**
     * Stops streamer.
     */
    public void stop() {
        if (dataStreamer != null)
            dataStreamer.close();
    }

    /**
     * Writes a Flume event.
     *
     * @param event Flume event.
     */
    protected void writeEvent(Event event) {
        Map<K, V> entries = transform(event);

        if (entries == null || entries.size() == 0)
            return;

        dataStreamer.addData(entries);
    }

    /**
     * Transforms {@link Event} into key-values.
     *
     * @param event Flume event.
     * @return Map of transformed key-values.
     */
    protected abstract Map<K, V> transform(Event event);
}
