package de.kp.works.ignite.core

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.IgniteConnect
import org.apache.ignite.Ignite
import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.cache.QueryEntity
import org.apache.ignite.configuration.CacheConfiguration

import java.util.concurrent.TimeUnit.SECONDS
import javax.cache.configuration.FactoryBuilder
import javax.cache.expiry.{CreatedExpiryPolicy, Duration}

abstract class BaseEngine(connect:IgniteConnect) {
  /*
   * The name of the temporary cache external events
   * are streamed to.
   */
  protected var cacheName:String
  protected val ignite: Ignite = connect.getIgnite

  def buildStream:Option[IgniteStreamContext]

  /**
   * Configuration for the events cache to store the stream
   * of events. This cache is configured with a sliding window
   * of N seconds, which means that data older than N second
   * will be automatically removed from the cache.
   */
  def createCacheConfig(timeWindow:Int):CacheConfiguration[String,BinaryObject] = {
    /*
     * Defining query entities is the Apache Ignite
     * mechanism to dynamically define a queryable
     * 'class'
     */
    val qes = new java.util.ArrayList[QueryEntity]()
    qes.add(buildQueryEntity)
    /*
     * Configure streaming cache, an Ignite cache used
     * to persist SSE temporarily
     */
    val cfg = new CacheConfiguration[String,BinaryObject]()
    cfg.setName(cacheName)
    /*
     * Specify Apache Ignite cache configuration; it is
     * important to leverage 'BinaryObject' as well as
     * 'setStoreKeepBinary'
     */
    cfg.setStoreKeepBinary(true)
    cfg.setIndexedTypes(classOf[String],classOf[BinaryObject])

    cfg.setQueryEntities(qes)

    cfg.setStatisticsEnabled(true)

    /* Sliding window of 'timeWindow' in seconds */
    val duration = timeWindow / 1000

    cfg.setExpiryPolicyFactory(FactoryBuilder.factoryOf(new CreatedExpiryPolicy(new Duration(SECONDS, duration))))
    cfg

  }

  /**
   * A helper method to build an Apache Ignite QueryEntity;
   * this entity reflects the format of an SSE message
   */
  protected def buildQueryEntity:QueryEntity = {

    val queryEntity = new QueryEntity()

    queryEntity.setKeyType("java.lang.String")
    queryEntity.setValueType(cacheName)

    val fields = buildFields()

    queryEntity.setFields(fields)
    queryEntity

  }

  protected def buildFields():java.util.LinkedHashMap[String,String]
  /**
   * This method deletes the temporary notification
   * cache from the Ignite cluster
   */
  protected def deleteCache():Unit = {
    try {

      if (ignite.cacheNames().contains(cacheName)) {
        val cache = ignite.cache(cacheName)
        cache.destroy()
      }

    } catch {
      case _:Throwable => /* do noting */

    }
  }

}
