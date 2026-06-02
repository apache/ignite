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

package org.apache.ignite.stream.scala.akka

import org.apache.ignite.IgniteDataStreamer
import org.apache.ignite.stream.{StreamMultipleTupleExtractor, StreamSingleTupleExtractor}

/**
 * Reimplementation StreamAdapter.java to trait.
 */
trait StreamAdapterTrait[T, K, V] {
    /** Streamer. */
    val strm: IgniteDataStreamer[K, V]

    /** Tuple extractor extracting a single tuple from an event */
    val singleTupleExtractor: StreamSingleTupleExtractor[T, K, V]

    /** Tuple extractor that supports extracting N tuples from a single event (1:n cardinality). */
    val multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]

    /**
     * Converts given message to 1 or many tuples (depending on the type of extractor) and adds it/them to the
     * underlying streamer.
     *
     * @param msg Message to convert.
     */
    def addMessage(msg: T): Unit = {
        if (multipleTupleExtractor == null) {
            val e = singleTupleExtractor.extract(msg)
            if (e != null) strm.addData(e)
        }
        else {
            val m = multipleTupleExtractor.extract(msg)
            if (m != null && !m.isEmpty) strm.addData(m)
        }
    }
}
