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
 * <p>
 * Two types of tuple extractors are supported:
 * <ol>
 *     <li>A single tuple extractor, which extracts either no or 1 tuple out of a message. See
 *     see {@link #setTupleExtractor(StreamTupleExtractor)}.</li>
 *     <li>A multiple tuple extractor, which is capable of extracting multiple tuples out of a single message, in the
 *     form of a {@link Map<K, V>}. See {@link #setMultipleTupleExtractor(StreamMultipleTupleExtractor)}.</li>
 * </ol>
 */
public abstract class StreamAdapter<T, K, V> {
    /** Tuple extractor. */
    private StreamTupleExtractor<T, K, V> extractor;

    /** Tuple extractor that supports extracting N tuples from a single event (1:n cardinality). */
    private StreamMultipleTupleExtractor<T, K, V> multipleTupleExtractor;

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
     * @return Provided tuple extractor (for 1:n cardinality).
     */
    public StreamMultipleTupleExtractor<T, K, V> getMultipleTupleExtractor() {
        return multipleTupleExtractor;
    }

    /**
     * @param multipleTupleExtractor Extractor for 1:n tuple extraction.
     */
    public void setMultipleTupleExtractor(StreamMultipleTupleExtractor<T, K, V> multipleTupleExtractor) {
        this.multipleTupleExtractor = multipleTupleExtractor;
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
     * Converts given message to 1 or many tuples (depending on the type of extractor) and adds it/them to the
     * underlying streamer.
     * <p>
     * If both a {@link #multipleTupleExtractor} and a {@link #extractor} have been set, the former will take precedence
     * and the latter will be ignored.
     *
     * @param msg Message to convert.
     */
    protected void addMessage(T msg) {
        if (multipleTupleExtractor == null) {
            Map.Entry<K, V> e = extractor.extract(msg);

            if (e != null)
                stmr.addData(e);

        } else {
            Map<K, V> m = multipleTupleExtractor.extract(msg);

            if (m != null)
                stmr.addData(m);

        }
    }

}