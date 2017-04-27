package org.apache.ignite.stream.scala.akka

import akka.Done
import akka.stream.scaladsl.Sink
import org.apache.ignite.IgniteDataStreamer
import org.apache.ignite.stream.{StreamAdapter, StreamMultipleTupleExtractor, StreamSingleTupleExtractor}

import scala.concurrent.{ExecutionContext, Future}

/**
 *
 * @tparam T
 * @tparam K
 * @tparam V
 */
class IgniteAkkaStreamer[T, K, V](
    val strm: IgniteDataStreamer[K, V],
    val singleTupleExtractor: StreamSingleTupleExtractor[T, K, V],
    val multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]
) extends StreamAdapter[T, K, V] {
    require(strm != null, "the IgniteDataStreamer must be initialize.")

    setStreamer(strm)

    require(singleTupleExtractor != null || multipleTupleExtractor != null, "the extractor must be initialize.")

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

    /**
     * Create foreach method.
     */
    def foreach: Sink[T, Future[Done]] = {
        require(getSingleTupleExtractor != null || getMultipleTupleExtractor != null, "the extractor must be initialize.")

        Sink.foreach((e: T) => {
            addMessage(e)
        })
    }

    /**
     * Create {@link Sink} foreachParallel method.
     *
     * @param threads Threads.
     * @param ec ExecutionContext object.
     */
    def foreachParallel(threads: Int, ec: ExecutionContext) {
        require(threads > 0, "threads has to larger than 0.")
        require(getSingleTupleExtractor != null || getMultipleTupleExtractor != null, "the extractor must be initialize.")

        Sink.foreachParallel(threads)((e: T) => {
                addMessage(e)
        })(ec)
    }
}
