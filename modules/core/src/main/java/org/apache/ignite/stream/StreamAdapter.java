/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
 *     form of a {@link Map}. See {@link #setMultipleTupleExtractor(StreamMultipleTupleExtractor)}.</li>
 * </ol>
 */
public abstract class StreamAdapter<T, K, V> {
    /** Tuple extractor extracting a single tuple from an event */
    private StreamSingleTupleExtractor<T, K, V> singleTupleExtractor;

    /** Tuple extractor that supports extracting N tuples from a single event (1:n cardinality). */
    private StreamMultipleTupleExtractor<T, K, V> multipleTupleExtractor;

    /** Streamer. */
    private IgniteDataStreamer<K, V> stmr;

    /** Ignite. */
    protected Ignite ignite;

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
     * @param extractor Tuple extractor (1:1).
     */
    protected StreamAdapter(IgniteDataStreamer<K, V> stmr, StreamSingleTupleExtractor<T, K, V> extractor) {
        this.stmr = stmr;
        this.singleTupleExtractor = extractor;
    }

    /**
     * Stream adapter.
     *
     * @param stmr Streamer.
     * @param extractor Tuple extractor (1:n).
     */
    protected StreamAdapter(IgniteDataStreamer<K, V> stmr, StreamMultipleTupleExtractor<T, K, V> extractor) {
        this.stmr = stmr;
        this.multipleTupleExtractor = extractor;
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
     * @see #getSingleTupleExtractor()
     */
    @Deprecated
    public StreamTupleExtractor<T, K, V> getTupleExtractor() {
        if (singleTupleExtractor instanceof StreamTupleExtractor)
            return (StreamTupleExtractor) singleTupleExtractor;

        throw new IllegalArgumentException("This method is deprecated and only relevant if using an old " +
            "StreamTupleExtractor; use getSingleTupleExtractor instead");
    }

    /**
     * @param extractor Extractor for a single key-value tuple from the message.
     * @see #setSingleTupleExtractor(StreamSingleTupleExtractor)
     */
    @Deprecated
    public void setTupleExtractor(StreamTupleExtractor<T, K, V> extractor) {
        if (multipleTupleExtractor != null)
            throw new IllegalArgumentException("Multiple tuple extractor already set; cannot set both types at once.");

        this.singleTupleExtractor = extractor;
    }

    /**
     * @return Provided single tuple extractor.
     */
    public StreamSingleTupleExtractor<T, K, V> getSingleTupleExtractor() {
        return singleTupleExtractor;
    }

    /**
     * @param singleTupleExtractor Extractor for key-value tuples from messages.
     */
    public void setSingleTupleExtractor(StreamSingleTupleExtractor<T, K, V> singleTupleExtractor) {
        if (multipleTupleExtractor != null)
            throw new IllegalArgumentException("Multiple tuple extractor already set; cannot set both types at once.");

        this.singleTupleExtractor = singleTupleExtractor;
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
        if (singleTupleExtractor != null)
            throw new IllegalArgumentException("Single tuple extractor already set; cannot set both types at once.");

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
     *
     * @param msg Message to convert.
     */
    protected void addMessage(T msg) {
        if (multipleTupleExtractor == null) {
            Map.Entry<K, V> e = singleTupleExtractor.extract(msg);

            if (e != null)
                stmr.addData(e);

        } else {
            Map<K, V> m = multipleTupleExtractor.extract(msg);
            
            if (m != null && !m.isEmpty())
                stmr.addData(m);

        }
    }

}
