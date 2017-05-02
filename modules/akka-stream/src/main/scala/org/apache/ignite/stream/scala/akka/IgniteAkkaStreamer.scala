package org.apache.ignite.stream.scala.akka

import akka.Done
import akka.stream.scaladsl.Sink
import org.apache.ignite.{Ignite, IgniteDataStreamer}
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
    // TODO А нужен ли он вообще?
    val ignite: Ignite,
    val singleTupleExtractor: StreamSingleTupleExtractor[T, K, V],
    val multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]
) extends StreamAdapter[T, K, V] {
    require(strm != null, "the IgniteDataStreamer must be initialize.")

    setStreamer(strm)
    setIgnite(ignite)

    require(singleTupleExtractor != null || multipleTupleExtractor != null, "the extractor must be initialize.")

    def this(
        strm: IgniteDataStreamer[K, V],
        ignite: Ignite,
        singleTupleExtractor: StreamSingleTupleExtractor[T, K, V]) {
        this(strm, ignite, singleTupleExtractor, null)
        setSingleTupleExtractor(singleTupleExtractor)
    }

    def this(
        strm: IgniteDataStreamer[K, V],
        ignite: Ignite,
        multipleTupleExtractor: StreamMultipleTupleExtractor[T, K, V]) {
        this(strm, ignite, null, multipleTupleExtractor)
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
