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

package org.apache.ignite.gatling.simulations

import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.protocol.IgniteProtocol

/**
 * Cache put simulation.
 */
class CachePutSimulation extends Simulation {
    val cacheName = "default"

    serverNode.getOrCreateCache(cacheName)

    val csvFeeder = csv("users.csv").circular

    val scn = scenario("Put Scenario")
        .feed(csvFeeder)
        .exec(ignite("Simple put").cache(cacheName).put("${id}", "${firstname}"))

    setUp(scn.inject(
        constantConcurrentUsers(5000) during 30
    )).protocols(new IgniteProtocol(serverNode))

    after(serverNode.close())
}


