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
import org.apache.ignite.stream.{StreamMultipleTupleExtractor, StreamSingleTupleExtractor}

/**
 * Implements actor for Ignite Streamer.
 *
 * @param stm Ignite data streamer.
 * @param ste Single tuple extractor.
 * @param mte Multiple tuple extractor.
 * @tparam T Message type.
 * @tparam K Key type.
 * @tparam V Value type.
 */
class IgniteAkkaActorStreamer[T, K, V](
    val stm: IgniteDataStreamer[K, V],
    val ste: StreamSingleTupleExtractor[T, K, V],
    val mte: StreamMultipleTupleExtractor[T, K, V]
) extends Actor with StreamAdapterTrait[T, K, V] {
    require(stm != null, "the IgniteDataStreamer must be initialize.")
    require(ste != null || mte != null, "the extractor must be initialize.")

    def receive = {
        case msg: T => {
            addMessage(msg)
        }
    }

    override val strm: IgniteDataStreamer[K, V] = stm
    override val singleTupleExtractor: StreamSingleTupleExtractor[T, K, V] = ste
    override val multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V] = mte
}
