package de.kp.works.ignite

/*
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

import org.apache.ignite._
import org.apache.ignite.binary.BinaryObject

object IgniteConnect {

  private var instance:Option[IgniteConnect] = None
  private var graphNS:Option[String] = None

  def getInstance(namespace: String): IgniteConnect = {

    graphNS = Some(namespace)

    if (instance.isEmpty)
      instance = Some(new IgniteConnect(namespace))

    instance.get

  }

  def namespace:String = {
    if (graphNS.isDefined) graphNS.get else null
  }
}

class IgniteConnect(val graphNS: String) {

  private var ignite:Ignite = getOrStart()

  def getIgnite: Ignite = ignite

  /**
   * A helper method to definitely ensure that
   * an Apache Ignite node is started
   */
  def getOrCreate:Ignite = {

    if (ignite == null) ignite = getOrStart()
    ignite

  }

  def getOrCreateCache(name: String): IgniteCache[String, BinaryObject] = {
    /*
     * .withKeepBinary() must not be used here
     */
    val cache = IgniteUtil.getOrCreateCache(ignite, name, graphNS)
    if (cache == null)
      throw new Exception("Connection to Ignited failed. Could not create cache.")
    /*
     * Rebalancing is called here in case of partitioned
     * Apache Ignite caches; the default configuration,
     * however, is to use replicated caches
     * cache.rebalance().get()
     */
    
    cache
  }

  def cacheExists(cacheName: String): Boolean = {
    ignite.cacheNames.contains(cacheName)
  }

  def deleteCache(cacheName: String): Unit = {
    val exists: Boolean = ignite.cacheNames.contains(cacheName)
    if (exists) {
      ignite.cache(cacheName).destroy()
    }
  }

  /**
   * Executing Apache Spark based dataframe write
   * operations requires to have an Apache Ignite
   * node started
   */
  def getOrStart(): Ignite = {
    Ignition.getOrStart(IgniteConf.fromConfig)
  }

}
