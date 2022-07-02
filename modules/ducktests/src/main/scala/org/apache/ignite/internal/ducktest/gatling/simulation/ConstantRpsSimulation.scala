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
package org.apache.ignite.internal.ducktest.gatling.simulation

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import io.gatling.core.Predef._
import org.apache.ignite.internal.ducktest.gatling.Profile
import org.apache.ignite.internal.ducktest.gatling.utils.DucktapeIgniteSupport

/**
 */
class ConstantRpsSimulation extends Simulation with DucktapeIgniteSupport {

  private val rps = Integer.getInteger("rps", 10)
  private val duration = Integer.getInteger("duration", 30)
  private val clazz: Class[() => Profile] = Class
    .forName(System.getProperty("profile", "org.apache.ignite.internal.ducktest.gatling.scenario.PutGetProfile"))
    .asInstanceOf[Class[() => Profile]]

  private val factory: () => Profile = clazz.getConstructor().newInstance()

  private val (ddl, (count, prepareTestData), loadTest) = factory()

  setUp(
    ddl
      .inject(atOnceUsers(1))
      .protocols(protocol)
      .andThen(
        prepareTestData
          .inject(rampUsers(count).during(10.seconds))
          .protocols(protocol)
          .andThen(loadTest.map { case (proportion, scenario) =>
            scenario
              .inject(
                rampUsersPerSec(0).to(rps.doubleValue() * proportion).during(10.seconds),
                constantUsersPerSec(rps.doubleValue() * proportion) during duration.seconds,
                rampUsersPerSec(rps.doubleValue() * proportion).to(0).during(10.seconds)
              )
              .protocols(protocol)
          }.toList: _*)
      )
  ).assertions(global.failedRequests.count.is(0))
}
