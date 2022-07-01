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
package org.apache.ignite.gatling.utils

import java.util.concurrent.atomic.AtomicInteger

import scala.util.Try

import io.gatling.core.feeder.Feeder
import io.gatling.core.feeder.Record
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef.igniteProtocol
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.IgnitionEx

/**
 * Gatling simulation mixin to support unit tests.
 */
trait IgniteSupport {
  /**
   * Helper feeder class generating long key and long value pairs.
   */
  class IntPairsFeeder extends Feeder[Int] {
    private val atomicInteger: AtomicInteger = new AtomicInteger(0)

    /**
     * @inheritdoc
     * @return @inheritdoc
     */
    override def hasNext: Boolean = true

    /**
     * @inheritdoc
     * @return @inheritdoc
     */
    override def next(): Record[Int] =
      Map("key" -> atomicInteger.incrementAndGet(), "value" -> atomicInteger.incrementAndGet())
  }

  /**
   * Helper feeder class generating batches of long key and long value pairs.
   */
  class BatchFeeder extends Feeder[Map[Int, Int]] {
    private val atomicInteger: AtomicInteger = new AtomicInteger(0)

    /**
     * @inheritdoc
     * @return @inheritdoc
     */
    override def hasNext: Boolean = true

    /**
     * @inheritdoc
     * @return @inheritdoc
     */
    override def next(): Record[Map[Int, Int]] =
      Map(
        "batch" -> Map(
          atomicInteger.incrementAndGet() -> atomicInteger.incrementAndGet(),
          atomicInteger.incrementAndGet() -> atomicInteger.incrementAndGet(),
          atomicInteger.incrementAndGet() -> atomicInteger.incrementAndGet()
        )
      )
  }

  /** Default feeder for unit tests. */
  protected val feeder: IntPairsFeeder = new IntPairsFeeder()

  /**
   * Setup Ignite protocol for gatling simulation used in unit tests.
   *
   * @return Ignite Protocol instance.
   */
  protected def protocol: IgniteProtocol =
    Option(System.getProperty("host"))
      .flatMap(host => Option(System.getProperty("port")).map(port => (host, port)))
      .map { case (host, port) =>
        Try(new ClientConfiguration().setAddresses(s"$host:$port"))
          .map(cfg => igniteProtocol.cfg(cfg).build)
          .getOrElse(igniteProtocol.cfg(IgnitionEx.allGrids().get(1)).build)
      }
      .getOrElse(igniteProtocol.cfg(IgnitionEx.allGrids().get(1)).build)
}
