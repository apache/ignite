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

import org.apache.flume.Event;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.stream.StreamAdapter;
import org.apache.ignite.stream.StreamMultipleTupleExtractor;

/**
 * Flume streamer that receives events from a sink and feeds key-value pairs into an {@link IgniteDataStreamer}.
 *
 * @param <T> Type of Flume Event.
 * @param <K> Type of cache key.
 * @param <V> Type of cache value.
 */
public class FlumeStreamer<T extends Event, K, V> extends StreamAdapter<T, K, V> {

    /** {@inheritDoc} */
    protected FlumeStreamer(IgniteDataStreamer<K, V> stmr, StreamMultipleTupleExtractor<T, K, V> extractor) {
        super(stmr, extractor);
    }

    /**
     * Writes a Flume event.
     *
     * @param event Flume event.
     */
    protected void writeEvent(T event) {
        addMessage(event);
    }
}
