package de.kp.works.ignite.core

/**
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import org.apache.ignite.IgniteCache
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.slf4j.LoggerFactory

abstract class IgniteProcessor(cache:IgniteCache[String,BinaryObject]) {

  private val LOGGER = LoggerFactory.getLogger(classOf[IgniteProcessor])

  protected val eventQuery:SqlFieldsQuery
  /**
   * These flags control shutdown mechanism
   */
  protected var stopped:Boolean = true
  protected var writing:Boolean = false
  /**
   * The frequency we flush the internal store and write
   * data to the predefined output is currently set to
   * 2 times of the stream buffer flush frequency
   */
  protected val flushWindow:Int

  protected var lastTs:Long = System.currentTimeMillis()

  /**
   * This method must be called once before 'execute'
   * is called; otherwise streaming does not start
   */
  def start():Unit = {
    stopped = false
  }

  def shutdown():Unit = {
    /*
     * Make sure that there is no further stream
     * processing (see 'execute')
     */
    stopped = true
    /*
     * Wait until writing cycle is finished
     * to stop the associated thread
     */
    while (writing) {}

  }

  /**
   * A helper method to apply the event query to the selected
   * Ignite cache, retrieve the results and write them to the
   * eventStore
   */
  protected def extractEntries():Unit
  /**
   * This method is responsible for retrieving the streaming
   * events, i.e. entries of the Apache Ignite stream cache
   */
  protected def readEvents():java.util.List[java.util.List[_]] = {
    /*
     * The default processing retrieves all entries of the
     * Apache Ignite stream cache without preprocessing
     */
    cache.query(eventQuery).getAll
  }
  /**
   * A helper method to process the extracted cache entries
   * and transform and write to predefined output
   */
  protected def processEntries():Unit

  def write():Unit = {
    /*
     * This flag prevents write processing after
     * shutdown is initiated
     */
    if (stopped) return

    /* Flag indicates that writing is started */
    writing = true

    val startTs = System.currentTimeMillis()
    /*
     * The total number of stream cache entries are defined as
     * 'batch'; 'batch' is a term that describes the physical
     * number of data records that take part in a certain
     * processing loop.
     *
     * Note, that there may be multiple processing loops on the
     * same (logical) cache entries as the cache expires after a
     * certain predefined period of time, and, the processing time
     * can be smaller than that period
     */
    val batchSize = cache.size()
    try {

      if (batchSize == 0) {
        /* Do nothing as this overloads external systems */

      } else {
        /* Extract cache entries and put to store */
        extractEntries()
        /*
         * Check whether we have to flush the stores, and write
         * the data records to the predefined output
         */
        if (startTs - lastTs > flushWindow) {

          lastTs = startTs
          /*
           * Flush data records to specified output
           */
          processEntries()
        }
      }

    } catch {
      case t:Throwable =>
        /*
         * In case of an error, we count all batch records as
         * errors, as we actually do not know how to measure
         * more sensitive
         */
        val message = "Streaming notification failed."
        LOGGER.error(message, t)
    }

    /* Flag indicates that writing cycle is finished */
    writing = false


  }
}