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
package org.apache.ignite.scalar.examples.spark

import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.configuration.{CacheConfiguration, IgniteConfiguration}
import org.apache.ignite.utils.closeAfter
import org.apache.ignite.{Ignite, IgniteCache, Ignition}
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

import scala.io.Source

/**
  * Example App to work with Ignite and Spark clusters.
  * Instructions to run:
  * <p>
  * # make current ignite release with: `mvn clean package -Pall-java,release -Dmaven.javadoc.skip=true`, `mvn initialize -Prelease`
  * # unpack apache-ignite-fabric into tmp folder and run 2 server nodes
  * # run `LoadCSVDataToIgnite` from IDE - IDEA spark plugin, right click on class name and Run
  * # download and unpack spark 2.2 distribution http://spark.apache.org/downloads.html
  * ## run ./sbin/start-master.sh (check http://localhost:8080 for spark GUI)
  * ## run ./sbin/start-slave.sh <SPARK-URL> (for me it a `spark://info-dep-564:7077` - printed in a master log at start)
  * # run `StandaloneClustersExample` from IDE
  * # ???
  * # PROFIT!!!
  */
object StandaloneClustersExample extends App {
    private val CONFIG = "examples/config/example-ignite-no-peer-clsldr.xml"

    private val MAVEN_HOME = "/home/dragon/.m2/repository"

    implicit val spark = SparkSession.builder()
        .appName("Spark Ignite data sources example")
        .master("spark://info-dep-564:7077")
        .getOrCreate()

    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-core/2.3.0-SNAPSHOT/ignite-core-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-spring/2.3.0-SNAPSHOT/ignite-spring-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-log4j/2.3.0-SNAPSHOT/ignite-log4j-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-spark/2.3.0-SNAPSHOT/ignite-spark-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/apache/ignite/ignite-indexing/2.3.0-SNAPSHOT/ignite-indexing-2.3.0-SNAPSHOT.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-beans/4.3.7.RELEASE/spring-beans-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-core/4.3.7.RELEASE/spring-core-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-context/4.3.7.RELEASE/spring-context-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/org/springframework/spring-expression/4.3.7.RELEASE/spring-expression-4.3.7.RELEASE.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/javax/cache/cache-api/1.0.0/cache-api-1.0.0.jar")
    spark.sparkContext.addJar(MAVEN_HOME + "/com/h2database/h2/1.4.195/h2-1.4.195.jar")

    searchRussianPlayers(spark)

    println("Search russian players...DONE")

    searchTopPlayedMatches(spark)

    println("Search players that played max matches...DONE")

    spark.close()

    def searchRussianPlayers(spark: SparkSession) = {

        val russianPlayers = spark.read
            .format("ignite")
            .option("config", CONFIG)
            .option("table", "player")
            .load().filter(col("country") === "RUS")

        russianPlayers.printSchema()
        russianPlayers.show()
    }

    def searchTopPlayedMatches(spark: SparkSession) = {
        spark.read
            .format("ignite")
            .option("config", CONFIG)
            .option("table", "player")
            .load().createOrReplaceTempView("player")

        spark.read
            .format("ignite")
            .option("config", CONFIG)
            .option("table", "match")
            .load().createOrReplaceTempView("match")

        val countMatches = spark.sql(
            """
            |  SELECT
            |    p.NAME,
            |    count(*) cnt
            |  FROM
            |      player p join
            |      match m on (p.PLAYER_ID = m.WINNER_ID) or (p.PLAYER_ID = m.LOSER_ID)
            |  GROUP BY p.NAME
            |  ORDER BY cnt DESC
            |  LIMIT 10
            """.stripMargin)

        countMatches.printSchema()
        countMatches.show()
    }
}

object LoadCSVDataToIgnite extends App {
    private val CONFIG = "examples/config/example-ignite.xml"

    private val CACHE_NAME = "testCache"

    closeAfter(setupClient) { client ⇒
        createTables(client)

        insertPlayers(client)

        insertMatches(client)
    }

    def insertMatches(client: Ignite): Unit = {
        val c = cache(client)

        val matches = new CSVReader("atp_matches_1970.csv")

        val idIterator = Iterator.from(1)

        println("Inserting matches...")
        val insertMatchQuery = new SqlFieldsQuery("INSERT INTO match " +
            "(id, winner_id, loser_id, tid, tname, draw_size, surface) VALUES (?, ?, ?, ?, ?, ?, ?)")
        for (m ← matches) {
            val matchArgs = List(
                idIterator.next,
                m("winner_id"),
                m("loser_id"),
                m("tourney_id"),
                m("tourney_name"),
                m("draw_size"),
                m("surface"))
            println(matchArgs.mkString("[", ",", "]"))

            c.query(insertMatchQuery.setArgs(matchArgs.map(_.asInstanceOf[AnyRef]): _*)).getAll
        }
        println("Inserting matches...[DONE]")
    }

    def insertPlayers(client: Ignite): Unit = {
        val c = cache(client)

        val uniqueWinners = new CSVReader("atp_matches_1970.csv").groupBy(_("winner_id")).map{ winner ⇒
            val w = winner._2.iterator.next
            List(
                w("winner_id"),
                w("winner_hand"),
                w("winner_ioc"),
                w("winner_name"))
        }

        val uniqueLosers = new CSVReader("atp_matches_1970.csv").groupBy(_("loser_id")).map{ loser ⇒
            val l = loser._2.iterator.next
            List(
                l("loser_id"),
                l("loser_hand"),
                l("loser_ioc"),
                l("loser_name"))
        }

        val uniquePlayers = (uniqueWinners ++ uniqueLosers).toList.distinct

        println("Inserting players...")
        val insertPlayerQry = new SqlFieldsQuery("INSERT INTO player (player_id, hand, country, name) " +
            "VALUES (?, ?, ?, ?)")

        for (p ← uniquePlayers) {
            println(p.mkString("[", ",", "]"))
            c.query(insertPlayerQry.setArgs(p.map(_.asInstanceOf[AnyRef]): _*)).getAll
        }
        println("Inserting players...[DONE]")

    }

    def createTables(client: Ignite) = {
        print("Creating tables...")
        if (!client.cacheNames().contains("SQL_PUBLIC_PLAYER")) {
            val c = cache(client)

            c.query(new SqlFieldsQuery(
                "CREATE TABLE player (player_id LONG PRIMARY KEY, hand VARCHAR, country VARCHAR, name VARCHAR)")).getAll

            c.query(new SqlFieldsQuery(
                "CREATE TABLE match (id LONG PRIMARY KEY, winner_id LONG, loser_id LONG, tid VARCHAR, tname VARCHAR, " +
                    "draw_size VARCHAR, surface VARCHAR)")).getAll
        }
        println("[DONE]")
    }

    def setupClient: Ignite = {
        print("Starting Ignite client...")
        val cfg = new IgniteConfiguration
        cfg.setClientMode(true)

        val ignite = Ignition.start(cfg)

        println("[DONE]")

        ignite
    }

    def cache(ignite: Ignite): IgniteCache[Any, Any] = {
        val ccfg = new CacheConfiguration[Any, Any](CACHE_NAME).setSqlSchema("PUBLIC")

        ignite.getOrCreateCache(ccfg)
    }

    class CSVReader(file: String) extends Iterable[Map[String, Any]] {
        private val lines = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream(file)).getLines

        private val names = lines.next().split(",")

        override def iterator: Iterator[Map[String, Any]] = new Iterator[Map[String, Any]] {
            override def hasNext = lines.hasNext

            override def next() =
                names.zip(lines.next().split(",")).toMap
        }
    }
}
