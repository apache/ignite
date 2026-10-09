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

import akka.actor.Actor
import org.apache.ignite.IgniteDataStreamer
import org.apache.ignite.stream.{StreamAdapter, StreamMultipleTupleExtractor, StreamSingleTupleExtractor}

/**
 * Implements actor for Ignite Streamer.
 *
 * @param strm Ignite data streamer.
 * @param singleTupleExtractor Single tuple extractor.
 * @param multipleTupleExtractor Multiple tuple extractor.
 * @tparam T Message type.
 * @tparam K Key type.
 * @tparam V Value type.
 */
class IgniteAkkaActorStreamerJava[T, K, V](
    val strm: IgniteDataStreamer[K, V],
    val singleTupleExtractor: StreamSingleTupleExtractor[T, K, V],
    val multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]
) extends StreamAdapter[T, K, V] with Actor {
    require(strm != null, "the IgniteDataStreamer must be initialize.")
    require(singleTupleExtractor != null || multipleTupleExtractor != null, "the extractor must be initialize.")

    setStreamer(strm)

    def this(
        strm: IgniteDataStreamer[K, V],
        singleTupleExtractor: StreamSingleTupleExtractor[T, K, V]) {
        this(strm, singleTupleExtractor, null)
        setSingleTupleExtractor(singleTupleExtractor)
    }

    def this(
        strm: IgniteDataStreamer[K, V],
        multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]) {
        this(strm, null, multipleTupleExtractor)
        setMultipleTupleExtractor(multipleTupleExtractor)
    }

    def receive = {
        case msg: T => {
            addMessage(msg)
        }
    }
}
