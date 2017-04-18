package org.apache.ignite.stream.scala.akka

import akka.actor.Actor
import org.apache.ignite.{Ignite, IgniteDataStreamer}
import org.apache.ignite.stream.{StreamMultipleTupleExtractor, StreamSingleTupleExtractor}

class IgniteStreamerActor[T, K, V](_strm: IgniteDataStreamer[K, V],
                                   _singleTupleExtractor: StreamSingleTupleExtractor[T, K, V],
                                   _multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V],
                                   _ignite: Ignite) extends Actor with StreamAdapterIgnite[T, K, V] {
    def receive = {
        case msg: T => {
            addMessage(msg)
            println(msg.toString)
        }
    }

    override var strm: IgniteDataStreamer[K, V] = _strm
    override var singleTupleExtractor: StreamSingleTupleExtractor[T, K, V] = _singleTupleExtractor
    override var multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V] = _multipleTupleExtractor
    override var ignite: Ignite = _ignite
}
