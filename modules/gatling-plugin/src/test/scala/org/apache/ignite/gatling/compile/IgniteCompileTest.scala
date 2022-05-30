package org.apache.ignite.gatling.compile

import org.apache.ignite.gatling.Predef.{simpleCheck, _}
import io.gatling.core.Predef._
import org.apache.ignite.{Ignite, Ignition}
import org.apache.ignite.cache.CacheEntryProcessor
import org.apache.ignite.client.{ClientCacheConfiguration, IgniteClient}
import org.apache.ignite.configuration.{CacheConfiguration, ClientConfiguration}
import org.apache.ignite.gatling.api.IgniteApi
import org.apache.ignite.gatling.protocol.IgniteProtocol

import java.util.concurrent.locks.Lock
import javax.cache.processor.MutableEntry

class IgniteCompileTest extends Simulation {
  private val thinProtocol: IgniteProtocol = ignite.cfg(new ClientConfiguration().setAddresses("localhost:10800"))
  private val nodeProtocol: IgniteProtocol = ignite.cfg(Ignition.start())

  private val scn = scenario("scn")
    .exec(ignite("ign").start)
    .exec(ignite("ign").close)

    .exec(ignite("ign").create("cache"))
    .exec(ignite("ign").create("cache").backups(1))
    .exec(ignite("ign").create("cache").atomicity(TRANSACTIONAL))
    .exec(ignite("ign").create("cache").mode(PARTITIONED))
    .exec(ignite("ign").create("cache").backups(1).atomicity(TRANSACTIONAL).mode(PARTITIONED))
    .exec(ignite("ign").create("cache").cfg(new ClientCacheConfiguration()))
    .exec(ignite("ign").create("cache").cfg(new CacheConfiguration[Int, Int]()))

    .exec(ignite("ign").txStart)
    .exec(ignite("ign").txStart(OPTIMISTIC, READ_COMMITTED))
    .exec(ignite("ign").txStart(OPTIMISTIC, READ_COMMITTED).timeout(1))
    .exec(ignite("ign").txStart(OPTIMISTIC, READ_COMMITTED).txSize(1))
    .exec(ignite("ign").txStart(OPTIMISTIC, READ_COMMITTED).timeout(1).txSize(1))

    .exec(ignite("ign").commit)
    .exec(ignite("ign").rollback)

    .exec(ignite("ign").cache("cache").put(1,2))
    .exec(ignite("ign").cache("cache").putAll(Map(1 -> 2, 3 -> 4)))
    .exec(ignite("ign").cache("cache").remove(1))
    .exec(ignite("ign").cache("cache").removeAll(Set(1)))
    .exec(ignite("ign").cache("cache").invoke[Int, Int, String](1)(
      new CacheEntryProcessor[Int, Int, String] {
        override def process(mutableEntry: MutableEntry[Int, Int], objects: Object*): String = ""
      })
    )
    .exec(ignite("ign").cache("cache").invoke[Int, Int, String](1).args(Seq("", 2, 3))
      { (_, _: Seq[Any]) =>  "" } )
    .exec(ignite("ign").cache("cache").invoke[Int, Int, String](1).args("", 2, Map(1 -> 3)) {
      (_, _: Seq[Any]) => ""
    })
    .exec(ignite("ign").cache("cache").invoke[Int, Int, String](1) {
      _: MutableEntry[Int, Int] => ""
    })
    .exec(ignite("ign").cache("cache").invoke[Int, Int, String](1)((_, _: Seq[Any]) =>  "")
      .check(
        allResults[Int, String].saveAs("R"),
        simpleCheck(result => result(1) == "")
      )
    )

    .exec(ignite("ign").cache("cache").getAll(Set(1)))
    .exec(ignite("ign").cache("cache").get(1))
    .exec(ignite("ign").cache("cache")
      .get[Int, Any](1)
      .check(
        allResults[Int, Any].transform(a => a.values.head).saveAs("R")
      )
    )
    .exec(ignite("ign").cache("cache")
      .get[Int, Int](1)
      .check(
        simpleCheck((result, session) => result(session("key").as[Int]) == session("value").as[Int])
      )
    )
    .exec(ignite("ign").cache("cache")
      .get[Int, Int](1)
      .check(
        simpleCheck(result => result(1) == 2)
      )
    )
    .exec(ignite("ign").cache("cache")
      .lock[Int](1)
      .check(
        allResults[Int, Lock].transform(a => a.values.head).saveAs("lock")
      )
    )
    .exec(ignite("ign").cache("cache").unlock("#{lock}"))

    .exec(session => {
      val client: IgniteApi = session("igniteApi").as[IgniteApi]

      val ignite: Ignite = client.wrapped
      ignite.close()
      val igniteClient: IgniteClient = client.wrapped
      igniteClient.close()

      session
    })
    .exec(session => {
      val client: IgniteApi = session("igniteApi").as[IgniteApi]
      val cache = client.cache[Int, Int]("test-cache").get
      cache.get(session("key").as[Int])(
        value => print(value)
      )
      client.close() {
        _ => client.txStart() {
          _ =>
        }
      }
      session
    })

  setUp(scn.inject(atOnceUsers(1)))
    .protocols(thinProtocol, nodeProtocol)
}
