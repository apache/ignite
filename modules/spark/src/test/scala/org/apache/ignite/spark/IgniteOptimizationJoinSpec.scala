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

package org.apache.ignite.spark

import org.apache.ignite.Ignite
import org.apache.ignite.cache.query.SqlFieldsQuery
import org.apache.ignite.internal.IgnitionEx
import org.apache.ignite.spark.AbstractDataFrameSpec.{DEFAULT_CACHE, TEST_CONFIG_FILE, checkOptimizationResult, enclose}
import org.apache.spark.sql.ignite.IgniteSparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import java.lang.{Long ⇒ JLong}

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationJoinSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Optimized join queries") {
        it("UNION") {
            val qry =
                """
                  | SELECT id, val1 as val FROM jt1 UNION
                  | SELECT id, val2 as val FROM jt2 UNION
                  | SELECT id, val3 as val FROM jt3
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT id, val FROM (SELECT id, val1 as val FROM jt1 UNION " +
                    "SELECT id, val2 as val FROM jt2 UNION " +
                    "SELECT id, val3 as val FROM jt3) table1")

            val data = (
                (1L, "A"),
                (1L, "B"),
                (2L, "B"),
                (2L, "C"),
                (2L, "D"),
                (3L, "C"),
                (3L, "D"),
                (3L, "E"))

            checkQueryData(df, data, row ⇒ (row.getAs[JLong](0), row.getAs[String](1)))
        }

        it("UNION ALL") {
            val qry =
                """
                  | SELECT id, val1 as val FROM jt1 UNION ALL
                  | SELECT id, val2 as val FROM jt2 UNION ALL
                  | SELECT id, val3 as val FROM jt3
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT id, val1 as val FROM jt1 UNION " +
                    "SELECT id, val2 as val FROM jt2 UNION " +
                    "SELECT id, val3 as val FROM jt3")

            val data = (
                (1L, "A"),
                (1L, "B"),
                (2L, "B"),
                (2L, "C"),
                (2L, "D"),
                (3L, "C"),
                (3L, "D"),
                (3L, "E"))

            checkQueryData(df, data, row ⇒ (row.getAs[JLong](0), row.getAs[String](1)))
        }

        it("UNION ALL ORDER") {
            val qry =
                """
                  | SELECT id, val1 as val FROM jt1 UNION ALL
                  | SELECT id, val2 as val FROM jt2 UNION ALL
                  | SELECT id, val3 as val FROM jt3
                  | ORDER BY id DESC, val
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT id, val1 as val FROM jt1 UNION " +
                    "SELECT id, val2 as val FROM jt2 UNION " +
                    "SELECT id, val3 as val FROM jt3 " +
                    "ORDER BY id DESC, val")

            val data = (
                (3L, "C"),
                (3L, "D"),
                (3L, "E"),
                (2L, "B"),
                (2L, "C"),
                (2L, "D"),
                (1L, "A"),
                (1L, "B")
            )

            checkQueryData(df, data, _ ⇒ 0)
        }

        it("UNION WITH AGGREGATE") {
            val qry =
                """
                  | SELECT VAL, COUNT(*) FROM (
                  |     SELECT id, val1 as val FROM jt1 UNION
                  |     SELECT id, val2 as val FROM jt2 UNION
                  |     SELECT id, val3 as val FROM jt3 ) t1
                  | GROUP BY val HAVING COUNT(*) > 1
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT VAL, count(1) FROM (" +
                    "SELECT id, val1 AS val FROM JT1 UNION " +
                    "SELECT id, val2 AS val FROM JT2 UNION " +
                    "SELECT id, val3 AS val FROM JT3" +
                ") table1 GROUP BY val HAVING count(1) > 1")

            val data = (
                ("B", 2L),
                ("C", 2L),
                ("D", 2L)
            )

            checkQueryData(df, data)
        }

        it("AGGREGATE ON AGGREGATE RESULT") {
            val qry =
                """
                  | SELECT SUM(cnt) FROM (
                  |     SELECT VAL, COUNT(*) as CNT FROM (
                  |         SELECT id, val1 as val FROM jt1 UNION
                  |         SELECT id, val2 as val FROM jt2 UNION
                  |         SELECT id, val3 as val FROM jt3 ) t1
                  |     GROUP BY val HAVING COUNT(*) > 1
                  | ) t1
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT CAST(SUM(cnt) as BIGINT) as \"SUM(cnt)\" FROM (" +
                    "SELECT count(1) as cnt FROM (" +
                        "SELECT id, val1 as val FROM jt1 UNION " +
                        "SELECT id, val2 as val FROM jt2 UNION " +
                        "SELECT id, val3 as val FROM jt3" +
                    ") table1 GROUP BY val HAVING count(1) > 1) table2")

            val data = Tuple1(6.0)

            checkQueryData(df, data)
        }

        it("SELF INNER JOIN") {
            val qry =
                """
                  |SELECT
                  | jt1.id,
                  | jt1.val1,
                  | jt2.id,
                  | jt2.val1
                  |FROM
                  |     jt1 JOIN
                  |     jt1 as jt2 ON jt1.val1 = jt2.val1
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df, "SELECT JT1.ID, JT1.VAL1, table1.ID, table1.VAL1 " +
                "FROM JT1 JOIN JT1 AS table1 ON jt1.val1 = table1.val1 " +
                "WHERE jt1.val1 IS NOT NULL AND table1.val1 IS NOT NULL")

            val data = (
                (1, "A", 1, "A"),
                (2, "B", 2, "B"),
                (3, "C", 3, "C")
            )

            checkQueryData(df, data)
        }


        it("SELF INNER JOIN WITH WHERE") {
            val qry =
                """
                  |SELECT
                  | jt1.id,
                  | jt1.val1,
                  | jt2.id,
                  | jt2.val1
                  |FROM
                  |     jt1 JOIN
                  |     jt1 as jt2 ON jt1.val1 = jt2.val1
                  |WHERE jt2.val1 = 'A'
                  |""".stripMargin

            val df = igniteSession.sql(qry)

/*            checkOptimizationResult(df, "SELECT JT1.ID, JT1.VAL1, table1.ID, table1.VAL1 " +
                "FROM JT1 JOIN JT1 as table1 ON JT1.val1 = table1.val1 " +
                "WHERE JT1.val1 = 'A' AND JT1.val1 IS NOT NULL AND table1.val1 IS NOT NULL AND table1.val1 = 'A'")*/

            val data = Tuple1(
                (1, "A", 1, "A")
            )

            checkQueryData(df, data)
        }


        it("INNER JOIN") {
            val qry =
                """
                  |SELECT
                  | jt1.id as id1,
                  | jt1.val1,
                  | jt2.id as id2,
                  | jt2.val2
                  |FROM
                  |     jt1 JOIN
                  |     jt2 ON jt1.val1 = jt2.val2
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df, "SELECT JT1.ID AS id1, JT1.VAL1, JT2.ID AS id2, JT2.VAL2 " +
                "FROM JT1 JOIN JT2 ON jt1.val1 = jt2.val2 " +
                "WHERE jt1.val1 IS NOT NULL AND jt2.val2 IS NOT NULL")

            val data = (
                (2, "B", 1, "B"),
                (3, "C", 2, "C")
            )

            checkQueryData(df, data)
        }

        it("INNER JOIN WITH WHERE") {
            val qry =
                """
                  |SELECT
                  | jt1.id as id1,
                  | jt1.val1,
                  | jt2.id as id2,
                  | jt2.val2
                  |FROM
                  |     jt1 JOIN
                  |     jt2 ON jt1.val1 = jt2.val2
                  |WHERE
                  | jt1.id < 10
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df, "SELECT jt1.id as id1, jt1.val1, jt2.id as id2, jt2.val2 " +
                "FROM jt1 JOIN jt2 ON jt1.val1 = jt2.val2 " +
                "WHERE jt1.id < 10 AND jt1.val1 IS NOT NULL and jt2.val2 IS NOT NULL")

            val data = (
                (2, "B", 1, "B"),
                (3, "C", 2, "C")
            )

            checkQueryData(df, data)
        }

        it("LEFT JOIN") {
            val qry =
                """
                  |SELECT
                  | jt1.id as id1,
                  | jt1.val1,
                  | jt2.id as id2,
                  | jt2.val2
                  |FROM
                  | jt1 LEFT JOIN
                  | jt2 ON jt1.val1 = jt2.val2
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df, "SELECT jt1.id as id1, jt1.val1, jt2.id as id2, jt2.val2 " +
                "FROM jt1 LEFT JOIN jt2 ON jt1.val1 = jt2.val2")

            val data = (
                (1, "A", null, null),
                (2, "B", 1, "B"),
                (3, "C", 2, "C")
            )

            checkQueryData(df, data)
        }

        it("RIGHT JOIN") {
            val qry =
                """
                  |SELECT
                  | jt1.id as id1,
                  | jt1.val1,
                  | jt2.id as id2,
                  | jt2.val2
                  |FROM
                  | jt1 RIGHT JOIN
                  | jt2 ON jt1.val1 = jt2.val2
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df, "SELECT jt1.id as id1, jt1.val1, jt2.id as id2, jt2.val2 " +
                "FROM jt1 RIGHT JOIN jt2 ON jt1.val1 = jt2.val2")

            val data = (
                (2, "B", 1, "B"),
                (3, "C", 2, "C"),
                (null, null, 3, "D")
            )

            checkQueryData(df, data, r ⇒ if (r.get(0) == null) 100L else r.getAs[Long](0))
        }

        it("JOIN 3 TABLE") {
            val qry =
                """
                  |SELECT
                  | jt1.id as id1,
                  | jt1.val1 as val1,
                  | jt2.id as id2,
                  | jt2.val2 as val2,
                  | jt3.id as id3,
                  | jt3.val3 as val3
                  |FROM
                  | jt1 LEFT JOIN
                  | jt2 ON jt1.val1 = jt2.val2 LEFT JOIN
                  | jt3 ON jt1.val1 = jt3.val3
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT table1.id as id1, table1.val1, table1.id_2 as id2, table1.val2, jt3.id as id3, jt3.val3 " +
                    "FROM (" +
                    "SELECT jt1.val1, jt1.id, jt2.val2, jt2.id as id_2 " +
                    "FROM JT1 LEFT JOIN jt2 ON jt1.val1 = jt2.val2) table1 LEFT JOIN " +
                    "jt3 ON table1.val1 = jt3.val3")

            val data = (
                (1, "A", null, null, 1, "A"),
                (2, "B", 1, "B", null, null),
                (3, "C", 2, "C", null, null))

            checkQueryData(df, data)
        }

        it("JOIN 3 TABLE AND AGGREGATE") {
            val qry =
                """
                  |SELECT SUM(id1) FROM (
                  | SELECT
                  |     jt1.id as id1,
                  |     jt1.val1 as val1,
                  |     jt2.id as id2,
                  |     jt2.val2 as val2,
                  |     jt3.id as id3,
                  |     jt3.val3 as val3
                  |FROM
                  |     jt1 LEFT JOIN
                  |     jt2 ON jt1.val1 = jt2.val2 LEFT JOIN
                  |     jt3 ON jt1.val1 = jt3.val3
                  |) WHERE CONCAT(val1, val2) = 'BB' OR CONCAT(val1, val3) = 'AA'
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
            "SELECT CAST(SUM(table1.ID) AS BIGINT) AS \"sum(id1)\" FROM " +
                "(SELECT JT1.VAL1, JT1.ID, JT2.VAL2 FROM JT1 LEFT JOIN JT2 ON JT1.val1 = JT2.val2) table1 LEFT JOIN " +
                "JT3 ON table1.val1 = JT3.val3 " +
                "WHERE CONCAT(table1.val1, table1.val2) = 'BB' OR CONCAT(table1.val1, JT3.val3) = 'AA'")

            val data = Tuple1(3)

            checkQueryData(df, data, _ ⇒ 0)
        }

        it("INNER JOIN SUBQUERY") {
            val qry =
                """
                  |SELECT sum_id, val1, val2 FROM (
                  |  SELECT
                  |    jt1.id + jt2.id as sum_id,
                  |    jt1.val1 as val1,
                  |    jt2.val2 as val2
                  |  FROM
                  |     jt1 JOIN
                  |     jt2 ON jt1.val1 = jt2.val2
                  |) t1 WHERE sum_id != 15
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT jt1.id + jt2.id as sum_id, jt1.val1, jt2.val2 FROM " +
                    "jt1 JOIN jt2 ON NOT jt1.id + jt2.id = 15 AND jt1.val1 = jt2.val2 " +
                    "WHERE " +
                    "jt1.val1 IS NOT NULL AND " +
                    "jt2.val2 IS NOT NULL"
            )

            val data = (
                (3, "B", "B"),
                (5, "C", "C")
            )

            checkQueryData(df, data)
        }

        it("INNER JOIN SUBQUERY - 2") {
            val qry =
                """
                  |SELECT SUM(sum_id) FROM (
                  |  SELECT
                  |    jt1.id + jt2.id as sum_id
                  |  FROM
                  |     jt1 JOIN
                  |     jt2 ON jt1.val1 = jt2.val2
                  |) t1 WHERE sum_id != 15
                  |""".stripMargin

            val df = igniteSession.sql(qry)

            checkOptimizationResult(df,
                "SELECT CAST(SUM(JT1.ID + JT2.ID) AS BIGINT) AS \"sum(sum_id)\" " +
                    "FROM JT1 JOIN JT2 ON NOT JT1.id + JT2.id = 15 AND JT1.val1 = JT2.val2 " +
                    "WHERE JT1.val1 IS NOT NULL AND JT2.val2 IS NOT NULL")

            val data = Tuple1(8)

            checkQueryData(df, data)
        }
    }

    def createJoinedTables(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE jt1 (
              |    id LONG,
              |    val1 VARCHAR,
              |    PRIMARY KEY (id)) WITH "backups=1"
            """.stripMargin)).getAll

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE jt2 (
              |    id LONG,
              |    val2 VARCHAR,
              |    PRIMARY KEY (id)) WITH "backups=1"
            """.stripMargin)).getAll

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE jt3 (
              |    id LONG,
              |    val3 VARCHAR,
              |    PRIMARY KEY (id)) WITH "backups=1"
            """.stripMargin)).getAll

        var qry = new SqlFieldsQuery("INSERT INTO jt1 (id, val1) values (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "A")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "B")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "C")).getAll

        qry = new SqlFieldsQuery("INSERT INTO jt2 (id, val2) values (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "B")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "C")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "D")).getAll

        qry = new SqlFieldsQuery("INSERT INTO jt3 (id, val3) values (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "A")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "D")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "E")).getAll

        cache.query(new SqlFieldsQuery("CREATE INDEX idx1 ON jt1(val1)")).getAll
        cache.query(new SqlFieldsQuery("CREATE INDEX idx2 ON jt2(val2)")).getAll
        cache.query(new SqlFieldsQuery("CREATE INDEX idx3 ON jt3(val3)")).getAll
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createPersonTable(client, DEFAULT_CACHE)

        createCityTable(client, DEFAULT_CACHE)

        createJoinedTables(client, DEFAULT_CACHE)

        val configProvider = enclose(null) (x ⇒ () ⇒ {
            val cfg = IgnitionEx.loadConfiguration(TEST_CONFIG_FILE).get1()

            cfg.setClientMode(true)

            cfg.setIgniteInstanceName("client-2")

            cfg
        })

        igniteSession = IgniteSparkSession.builder()
            .config(spark.sparkContext.getConf)
            .igniteConfigProvider(configProvider)
            .getOrCreate()
    }
}
