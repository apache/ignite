package org.apache.ignite.stream.scala.akka

import org.apache.ignite.{Ignite, IgniteDataStreamer}
import org.apache.ignite.stream.{StreamMultipleTupleExtractor, StreamSingleTupleExtractor}

trait StreamAdapterIgnite[T, K, V] {
    var strm: IgniteDataStreamer[K, V]

    var singleTupleExtractor: StreamSingleTupleExtractor[T, K, V]

    var multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]

    var ignite: Ignite

    def setStreamer(strm: IgniteDataStreamer[K, V]): Unit = {
        this.strm = strm
    }

    def getSingleTupleExtractor(): StreamSingleTupleExtractor[T, K, V] = {
        return singleTupleExtractor
    }

    def setSingleTupleExtractor(singleTupleExtractor: StreamSingleTupleExtractor[T, K, V]): Unit = {
        this.singleTupleExtractor = singleTupleExtractor
    }

    def getMultipleTupleExtractor(): StreamMultipleTupleExtractor[T, K, V] = {
        return multipleTupleExtractor
    }

    def setMultipleTupleExtractor(multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]): Unit = {
        this.multipleTupleExtractor = multipleTupleExtractor
    }

    def getStreamer(): IgniteDataStreamer[K, V] = {
        return strm
    }

    def setIgnite(ignite: Ignite): Unit = {
        this.ignite = ignite
    }

    def getIgnite(): Ignite = {
        return ignite
    }

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
