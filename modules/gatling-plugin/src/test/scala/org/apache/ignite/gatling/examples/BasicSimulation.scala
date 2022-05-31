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

package org.apache.ignite.gatling.examples

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration.DurationInt

import io.gatling.core.Predef._
import io.gatling.core.feeder.Feeder
import io.gatling.core.structure.ChainBuilder
import io.gatling.core.structure.ScenarioBuilder
import org.apache.ignite.Ignition
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.allResults
import org.apache.ignite.gatling.protocol.IgniteThinProtocolBuilder

class BasicSimulation extends Simulation {
  //  val protocol: IgniteProtocol = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))

  val c = new AtomicInteger(0)
  val feeder: Feeder[Int] = Iterator.continually(Map(
    "key" -> c.incrementAndGet(),
    "value" -> c.incrementAndGet()))

  //  val e = exec(ignite("Start client").start)

  val scn: ScenarioBuilder = scenario("Basic")
    .feed(feeder)
    //    .exec(e)
    .exec(ignite("Start client").start)
    .exec(ignite("Create cache").create("TEST-CACHE").backups(1))
    .exec(ignite("Put").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
    .exec(ignite("Get absent")
      .cache("TEST-CACHE")
      .get[Int, Int](100000)
      .check(
        simpleCheck(m => {
          //          printf("m=%s\n", m.toString)
          m.get(100000) == null
        })
      ))
    .exec {
      session =>
        println(session);
        session
    }
    .exec(ignite("Get present")
      .cache("TEST-CACHE")
      .get[Int, Int]("#{key}")
      //      .check(allResults[Int, Int].
      .check(
        //        simpleCheck((m, s) => m.contains(s("key").as[Int])),
        //        simpleCheck(m => m.contains(3)),
        simpleCheck((m, session) => {
          println(m)
          println(session)
          true
          //          m.get(session("key").as[Int]).get == null
        }),
        allResults[Int, Int].saveAs("R"),
        simpleCheck((m, session) => {
          println(m)
          println(session)
          true
          //          m.get(session("key").as[Int]).get == null
        })
        //        allResults[Int, Int].name("aaa"),
        //        allRecordsCheck(responseTimeInMillis.lt(1))
      )

    )
    .exec {
      session =>
        println(session)
        session
    }
    .exec(ignite("Close client").close)


  val commitTx: ChainBuilder =
    exec(ignite("txStart-1").txStart)
      .exec(ignite("put-1").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      .exec(ignite("commit").commit)
      .exec(ignite("get after commit")
        .cache("TEST-CACHE")
        .get[Int, Any]("#{key}")
        .check(
          allResults[Int, Any].saveAs("C"),
          simpleCheck((m, session) => {
            m(session("key").as[Int]) == session("value").as[Int]
          })
        )
      )
      .exec { session => println(session); session }

  val rollbackTx: ChainBuilder =
    exec(ignite("txStart-2").txStart)
      .exec(ignite("put-2").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
      //  exec(ignite("put-2").cache("TEST-CACHE").put[Int, Int](3456, "#{value}"))
      .exec(ignite("rollback").rollback)
      //      .exec(pause(1000.milliseconds))
      .exec(ignite("get after rollback")
        .cache("TEST-CACHE")
        .get[Int, Any]("#{key}")
        .check(
          simpleCheck((m, session) => {
            println(m)
            //          true
            m(session("key").as[Int]) == null
          }),
          allResults[Int, Any].saveAs("R")
        )
      )
      .exec { session => println(session); session }

  val longScn: ScenarioBuilder = scenario("long")
    .feed(feeder)
    .exec(ignite("Start client").start)
    .exec(ignite("Create cache").create("TEST-CACHE").backups(0).atomicity(TRANSACTIONAL))


    //  .exec(ignite("put-2").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
    //    //    .exec(ignite("rollback").rollback)
    //    .exec(ignite("get after rollback")
    //      .cache("TEST-CACHE")
    //      .get[Int, Int]("#{key}")
    //      .check(
    //        simpleCheck((m, session) => {
    //          println(m)
    //          println(session)
    //          true
    //          //          m.get(session("key").as[Int]).get == null
    //        }),
    //        allResults[Int, Int].saveAs("R"),
    //        simpleCheck((m, session) => {
    //          println(m)
    //          println(session)
    //          true
    //          //          m.get(session("key").as[Int]).get == null
    //        }),
    //      )
    //    )
    //    .exec ({ session => println(session); session })

    .exec(rollbackTx)


    .exec(commitTx)

    //    .during(10.seconds) {
    //        //    .exec(e)
    //        pace(100.milliseconds)
    //          .exec(ignite("txStart").tx)
    ////          .exec(ignite("txStart").tx (PESSIMISTIC, REPEATABLE_READ))
    ////          .exec(ignite("txStart").tx (PESSIMISTIC, REPEATABLE_READ) timeout 123)
    ////          .exec(ignite("txStart").tx (PESSIMISTIC, REPEATABLE_READ) timeout 123 txSize(10))
    //          .exec(ignite("Put").cache("TEST-CACHE").put[Int, Int]("#{key}", "#{value}"))
    //          .exec(ignite("Get absent")
    //            .cache("TEST-CACHE")
    //            .get[Int, Int](100000)
    //            .check(
    //              simpleCheck(m => {
    //                m.get(100000) == null
    //              })
    //            ))
    //          .exec(ignite("Get present")
    //            .cache("TEST-CACHE")
    //            .get[Int, Int]("#{key}")
    //            .check(
    //              allResults[Int, Int].saveAs("R"),
    //            )
    //          )
    //          .exec(ignite("commit").commit)
    //    }
    .exec(ignite("Close client").close)


  before {
    Ignition.start()
  }
  after {
    Ignition.allGrids().get(0).close()
  }

  val protocol: IgniteThinProtocolBuilder = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))

  setUp(
    longScn
      .inject(
        rampUsers(2).during(5.seconds)
      )
    //    scn
    //      .inject(
    //        constantUsersPerSec(1000) during 10,
    //        constantUsersPerSec(2) during 1,
    //        nothingFor(1),
    //        constantUsersPerSec(4) during 1,
    //        incrementUsersPerSec(1).times(2).eachLevelLasting(1)
    //      )
  )
    .protocols(protocol)
    .maxDuration(600.seconds)
    .assertions(global.failedRequests.count.is(0))
}
