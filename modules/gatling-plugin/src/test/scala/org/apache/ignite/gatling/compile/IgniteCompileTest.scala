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
package org.apache.ignite.gatling.compile

import java.util.concurrent.locks.Lock

import javax.cache.processor.MutableEntry

import io.gatling.core.Predef._
import io.gatling.core.structure.ChainBuilder
import org.apache.ignite.Ignite
import org.apache.ignite.Ignition
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.client.ClientCacheConfiguration
import org.apache.ignite.client.IgniteClient
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.configuration.ClientConfiguration
import org.apache.ignite.gatling.Predef._
import org.apache.ignite.gatling.Predef.group
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.api.node.IgniteNodeApi
import org.apache.ignite.gatling.api.thin.IgniteThinApi
import org.apache.ignite.gatling.protocol.IgniteProtocol

/**
 * Tests any combinations of Ignite Gatling DSL syntax.
 */
class IgniteCompileTest extends Simulation {
  private val thinProtocol: IgniteProtocol = igniteProtocol.cfg(new ClientConfiguration().setAddresses("localhost:10800"))
  private val nodeProtocol: IgniteProtocol = igniteProtocol.cfg(Ignition.start())

//  private val scnEx = scenario("scn")
//    .feed(IntPairsFeeder())
//    .exec(ignite {
//      exec(ignite("ign").cache("cache").put(1, 2))
//        .exec(ignite("ign").cache("cache").putPair((1, 2)))
//    })

  private val r = "request"
  private val c = "cache"
//  private val scn1: = scenario("scn")
//    .asIgnite
//    .exec("start" start)

  private val igniteChainBuilder: ChainBuilder = ignite(
    tx("")(
      start as "start client",
      put(c, 1, 2) as "put",
//        feed(IntPairsFeeder()),
      commit as "commit"
    ),
    exec(start as r),
    put("test-cache", "#{key}", "#{value}") as "put request",
//      .feed(IntPairsFeeder())
    tx("")(
      exec(start as r)
    ),
    group("gn") {
      exec(start as r)
    },
    tx("tx start")(
      exec(start as r)
        .exec(close as r)
    )
//    .put("")("", 1, 2)
//    .put("")("", 1, 2)
    ,
    tx("tx 2")(
      start as r,
      close as r
    )
  )
    .exec(start as "start")

//    .exec(tx("tx") (
//      exec(ignite(r).cache(c).put(1, 2))
//        .exec(ignite(r).cache(c).putPair((1, 2)))
//    ))
    .exec(start as r)
    .exec(close as r)
    .exec(create(c) as r)
    .exec(create(c) as r)
    .exec(create(c).backups(1) as r)
    .exec(create(c).atomicity(TRANSACTIONAL) as r)
    .exec(create(c).mode(PARTITIONED) as r)
    .exec(create(c).backups(1).atomicity(TRANSACTIONAL).mode(PARTITIONED) as r)
    .exec(create(c).cfg(new ClientCacheConfiguration()) as r)
    .exec(create(c).cfg(new CacheConfiguration[Int, Int]()) as r)
    .exec(tx())
    .exec(tx(OPTIMISTIC, READ_COMMITTED)(commit))
    .exec(tx(OPTIMISTIC, READ_COMMITTED).timeout(1L)(rollback))
    .exec(tx(OPTIMISTIC, READ_COMMITTED).txSize(1)())
    .exec(tx(OPTIMISTIC, READ_COMMITTED).timeout(1L).txSize(1)())
    .exec(tx("t")())
    .exec(tx("t")(OPTIMISTIC, READ_COMMITTED)(commit))
    .exec(tx("t")(OPTIMISTIC, READ_COMMITTED).timeout(1L)(rollback))
    .exec(tx("t")(OPTIMISTIC, READ_COMMITTED).txSize(1)())
    .exec(tx("t")(OPTIMISTIC, READ_COMMITTED).timeout(1L).txSize(1)())
    .exec(commit)
    .exec(rollback)
    .exec(commit as "commit")
    .exec(rollback as "rollback")
    .exec(put(c, 1, 2))
    .exec(put(c, (1, 2)))
    .exec(put[Int, Int](c, "#{key}", "#{value}"))
    .exec(put(c, (1, 2)))
    .exec(put(c, 1, 2).keepBinary)
    .exec(putAll(c, Map(1 -> 2, 3 -> 4)))
    .exec(remove(c, 1))
    .exec(removeAll(c, Set(1)))
    .exec(invoke[Int, Int, String](c, 1)(new CacheEntryProcessor[Int, Int, String] {
      override def process(mutableEntry: MutableEntry[Int, Int], objects: Object*): String = ""
    }))
    .exec(invoke[Int, Int, String](c, 1).args(Seq("", 2, 3))((_, _: Seq[Any]) => ""))
    .exec(invoke[Int, Int, String](c, 1).args("", 2, Map(1 -> 3)) { (_, _: Seq[Any]) =>
      ""
    })
    .exec(invoke[Int, Int, String](c, 1) { _: MutableEntry[Int, Int] =>
      ""
    })
    .exec(
      invoke[Int, Int, String](c, 1)((_, _: Seq[Any]) => "")
        .check(
          mapResult[Int, String].saveAs("R1"),
          mapResult[Int, String].validate((result: Map[Int, String], _: Session) => result(1) == "")
        ) as r
    )
    .exec(getAll(c, Set(1)))
    .exec(get(c, 1) as r)
    .exec(get(c, 1).keepBinary as "s")
    .exec(
      get[Int, Any](c, 1).keepBinary
        .check(
          mapResult[Int, Any].transform(a => a.values.head).saveAs("R")
        ) as "aa"
    )
    .exec(
      get[Int, Int](c, 1)
        .check(
          mapResult[Int, Int].validate((result: Map[Int, Int], session: Session) =>
            result(session("key").as[Int]) == session("value").as[Int]
          )
        )
    )
    .exec(
      get[Int, Int](c, 1)
        .check(
          mapResult[Int, Int].validate((result: Map[Int, Int], _: Session) => result(1) == 2)
        )
    )
    .exec(getAndRemove(c, 1))
    .exec(
      getAndRemove[Int, Any](c, 1)
        .check(
          mapResult[Int, Any].transform(a => a.values.head).saveAs("R2")
        )
    )
    .exec(getAndPut(c, 1, 2))
    .exec(
      getAndPut[Int, Any](c, 1, 2)
        .check(
          mapResult[Int, Any].transform(a => a.values.head).saveAs("R3"),
          mapResult[Int, Any].exists,
          entries[Int, Any].count.is(1)
        )
    )
    .exec(
      lock[Int](c, 1)
        .check(
          mapResult[Int, Lock].transform(a => a.values.head).saveAs("lock")
        )
    )
    .exec(unlock(c, "#{lock}"))
    .exec(sql(c, "CREATE TABLE TEST_TABLE"))
    .exec(
      sql(c, "SELECT * FROM TEST_TABLE WHERE id = ?")
        .args("#{id}")
    )
    .exec(
      sql(c, "SELECT * FROM TEST_TABLE WHERE id = ? AND affinity_id = ?")
        .args("#{id}", "#{affinity_id}")
    )
    .exec(
      sql(c, "SELECT * FROM TEST_TABLE")
        .partitions("#{partitions}")
    )
    .exec(
      sql(c, "SELECT * FROM TEST_TABLE WHERE id = ? AND affinity_id = ?")
        .check(resultSet.findAll.transformWithSession((r, s) => s("value").as[Int] :: r.head).saveAs("firstRow"))
    )
    .exec { session =>
      val api: IgniteApi = session("igniteApi").as[IgniteApi]
      api match {
        case IgniteNodeApi(w) => w.close()
        case IgniteThinApi(z) => z.close()
      }
      session
    }
    .exec { session =>
      val ignite: Ignite = session("igniteApi").as[IgniteNodeApi].wrapped
      ignite.close()
      session
    }
    .exec { session =>
      val igniteClient: IgniteClient = session("igniteApi").as[IgniteThinApi].wrapped
      igniteClient.close()
      session
    }
    .exec { session =>
      val client: IgniteApi = session("igniteApi").as[IgniteApi]
      val cache = client.cache[Int, Int]("test-cache").toOption.get
      cache.get(session("key").as[Int])(value => print(value))
      client.close() { _ =>
        client.txStart()({ _ => }, { _ => })
      }
      session
    }

  private val scn = scenario("scn").exec(igniteChainBuilder)
  setUp(scn.inject(atOnceUsers(1)))
    .protocols(thinProtocol, nodeProtocol)
}
