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

package org.apache.ignite.gatling.simulation

import java.util.concurrent.atomic.AtomicInteger

import scala.util.Try

import io.gatling.core.feeder.Feeder
import io.gatling.core.feeder.Record
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol
import org.apache.ignite.internal.IgnitionEx

trait IgniteSupport {

  case class IntPairsFeeder(atomicInteger: AtomicInteger = new AtomicInteger(0)) extends Feeder[Int] {
    override def hasNext: Boolean = true

    override def next(): Record[Int] =
      Map("key" -> atomicInteger.incrementAndGet(), "value" -> atomicInteger.incrementAndGet())
  }

  protected def feeder: IntPairsFeeder = IntPairsFeeder()

  protected def protocol: IgniteProtocol = {
    Option(System.getProperty("host"))
      .flatMap(host => Option(System.getProperty("port")).map(port => (host, port)))
      .map { case (host, port) =>
        Try(new ClientConfiguration().setAddresses(s"$host:$port"))
          .map(cfg => ignite.cfg(cfg).build)
          .getOrElse(ignite.cfg(IgnitionEx.allGrids().get(1)).build)
      }
      .getOrElse(ignite.cfg(IgnitionEx.allGrids().get(1)).build)
  }
}
