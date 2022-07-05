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
import org.apache.ignite.internal.ducktest.gatling.profile.Profile
import org.apache.ignite.internal.ducktest.gatling.utils.DucktapeIgniteSupport

/**
 * Generic simulation executing scenarios taken from the profile.
 *
 * Takes external configuration via the system properties:
 *
 * - profile: class name of the profile
 *
 * - rps: base frequency of the execution
 *
 * - duration: duration of the execution in seconds
 *
 * - step: particular profile scenario to execute (ddl, data, test).
 */
class ProfileAwareSimulation extends Simulation with DucktapeIgniteSupport {
  private val profile: Profile = Class
    .forName(System.getProperty("profile", "org.apache.ignite.internal.ducktest.gatling.profile.SampleProfile"))
    .asInstanceOf[Class[Profile]]
    .getConstructor()
    .newInstance()
  private val rps = Integer.getInteger("rps", 10)
  private val duration = Integer.getInteger("duration", 30)
  private val step = System.getProperty("step", "test")

  private val injectionProfile = step match {
    case "ddl"  => List(profile.ddl.inject(atOnceUsers(1)).protocols(protocol))
    case "data" => List(profile.data._2.inject(rampUsers(profile.data._1).during(10.seconds)).protocols(protocol))
    case "test" =>
      profile.test.map { case (proportion, scenario) =>
        scenario
          .inject(
            constantUsersPerSec(rps.doubleValue() * proportion / 100.0) during duration.seconds
          )
          .protocols(protocol)
      }.toList
  }

  setUp(injectionProfile: _*).assertions(global.failedRequests.count.is(0))
}
