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

package org.apache.ignite.gatling.action.cache

import io.gatling.core.Predef._
import org.apache.ignite.gatling.Predef._

class GetSimulation extends IgniteTestSimulation {

  val cache = "TEST-CACHE"

  private val scn = scenario("Basic")
    .feed(feeder)
    .exec(ignite("start").start)
    .exec(ignite("create").create(cache) backups 1  atomicity ATOMIC mode PARTITIONED)
    .exec(ignite("put").cache(cache).put[Int, Int]("#{key}", "#{value}"))
    .exec(ignite("get absent").cache(cache).get[Int, Any](key = -2)
      check allResults[Int, Any].transform(r => r(-2)).isNull)
    .exec(session => {
      logger.info(session.toString)
      session
    })
    .exec((ignite("get present").cache(cache).get[Int, Int](key = "#{key}"))
      check(
      simpleCheck((r, s) => r(s("key").as[Int]) == s("value").as[Int]),
      allResults[Int, Int].saveAs("savedInSession"))
    )

  setUp(scn.inject(atOnceUsers(1))).protocols(protocol).assertions(global.failedRequests.count.is(0))
}
