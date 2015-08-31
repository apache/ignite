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

package org.apache.ignite.stream;

import java.util.Map;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;

/**
 * Convenience adapter for streamers. Adapters are optional components for
 * streaming from different data sources. The purpose of adapters is to
 * convert different message formats into Ignite stream key-value tuples
 * and feed the tuples into the provided {@link org.apache.ignite.IgniteDataStreamer}.
 */
public abstract class StreamAdapter<T, K, V> {
    /** Tuple extractor. */
    private StreamTupleExtractor<T, K, V> extractor;

    /** Streamer. */
    private IgniteDataStreamer<K, V> stmr;

    /** Ignite. */
    private Ignite ignite;

    /**
     * Empty constructor.
     */
    protected StreamAdapter() {
        // No-op.
    }

    /**
     * Stream adapter.
     *
     * @param stmr Streamer.
     * @param extractor Tuple extractor.
     */
    protected StreamAdapter(IgniteDataStreamer<K, V> stmr, StreamTupleExtractor<T, K, V> extractor) {
        this.stmr = stmr;
        this.extractor = extractor;
    }

    /**
     * @return Provided data streamer.
     */
    public IgniteDataStreamer<K, V> getStreamer() {
        return stmr;
    }

    /**
     * @param stmr Ignite data streamer.
     */
    public void setStreamer(IgniteDataStreamer<K, V> stmr) {
        this.stmr = stmr;
    }

    /**
     * @return Provided tuple extractor.
     */
    public StreamTupleExtractor<T, K, V> getTupleExtractor() {
        return extractor;
    }

    /**
     * @param extractor Extractor for key-value tuples from messages.
     */
    public void setTupleExtractor(StreamTupleExtractor<T, K, V> extractor) {
        this.extractor = extractor;
    }

    /**
     * @return Provided {@link Ignite} instance.
     */
    public Ignite getIgnite() {
        return ignite;
    }

    /**
     * @param ignite {@link Ignite} instance.
     */
    public void setIgnite(Ignite ignite) {
        this.ignite = ignite;
    }

    /**
     * Converts given message to a tuple and adds it to the underlying streamer.
     *
     * @param msg Message to convert.
     */
    protected void addMessage(T msg) {
        Map.Entry<K, V> e = extractor.extract(msg);

        if (e != null)
            stmr.addData(e);
    }
}