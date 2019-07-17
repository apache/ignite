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
  * === Doesn't supported by Spark ===
  * CHAR
  * DIFFERENCE
  * HEXTORAW
  * RAWTOHEX
  * REGEXP_LIKE
  * SOUNDEX
  * STRINGDECODE
  * STRINGENCODE
  * STRINGTOUTF8
  * UTF8TOSTRING
  * XMLATTR
  * XMLNODE
  * XMLCOMMENT
  * XMLCDATA
  * XMLSTARTDOC
  * XMLTEXT
  * TO_CHAR - The function that can format a timestamp, a number, or text.
  * ====== This functions in spark master but not in release =====
  * LEFT
  * RIGHT
  * INSERT
  * REPLACE
  */
@RunWith(classOf[JUnitRunner])
class IgniteOptimizationStringFuncSpec extends AbstractDataFrameSpec {
    var igniteSession: IgniteSparkSession = _

    describe("Supported optimized string functions") {
        it("LENGTH") {
            val df = igniteSession.sql("SELECT LENGTH(str) FROM strings WHERE id <= 3")

            checkOptimizationResult(df, "SELECT CAST(LENGTH(str) AS INTEGER) as \"length(str)\" FROM strings " +
                "WHERE id <= 3")

            val data = (3, 3, 6)

            checkQueryData(df, data)
        }

        it("RTRIM") {
            val df = igniteSession.sql("SELECT RTRIM(str) FROM strings WHERE id = 3")

            checkOptimizationResult(df, "SELECT RTRIM(str) FROM strings WHERE id = 3")

            val data = Tuple1("AAA")

            checkQueryData(df, data)
        }

        it("RTRIMWithTrimStr") {
            val df = igniteSession.sql("SELECT RTRIM('B', str) FROM strings WHERE id = 9")

            checkOptimizationResult(df, "SELECT RTRIM(str, 'B') FROM strings WHERE id = 9")

            val data = Tuple1("BAAA")

            checkQueryData(df, data)
        }

        it("LTRIM") {
            val df = igniteSession.sql("SELECT LTRIM(str) FROM strings WHERE id = 4")

            checkOptimizationResult(df, "SELECT LTRIM(str) FROM strings WHERE id = 4")

            val data = Tuple1("AAA")

            checkQueryData(df, data)
        }

        it("LTRIMWithTrimStr") {
            val df = igniteSession.sql("SELECT LTRIM('B', str) FROM strings WHERE id = 9")

            checkOptimizationResult(df, "SELECT LTRIM(str, 'B') FROM strings WHERE id = 9")

            val data = Tuple1("AAAB")

            checkQueryData(df, data)
        }

        it("TRIM") {
            val df = igniteSession.sql("SELECT TRIM(str) FROM strings WHERE id = 5")

            checkOptimizationResult(df, "SELECT TRIM(str) FROM strings WHERE id = 5")

            val data = Tuple1("AAA")

            checkQueryData(df, data)
        }

		it("TRIMWithTrimStr") {
			val df = igniteSession.sql("SELECT TRIM('B', str) FROM strings WHERE id = 9")

			checkOptimizationResult(df, "SELECT TRIM(str, 'B') FROM strings WHERE id = 9")

			val data = Tuple1("AAA")

			checkQueryData(df, data)
		}

		it("TRIMWithTrimStrBOTH") {
			val df = igniteSession.sql("SELECT TRIM(BOTH 'B' FROM str) FROM strings WHERE id = 9")

			checkOptimizationResult(df, "SELECT TRIM(str, 'B') FROM strings WHERE id = 9")

			val data = Tuple1("AAA")

			checkQueryData(df, data)
		}

		it("TRIMWithTrimStrLEADING") {
			val df = igniteSession.sql("SELECT TRIM(LEADING 'B' FROM str) FROM strings WHERE id = 9")

			checkOptimizationResult(df, "SELECT LTRIM(str, 'B') FROM strings WHERE id = 9")

			val data = Tuple1("AAAB")

			checkQueryData(df, data)
		}

		it("TRIMWithTrimStrTRAILING") {
			val df = igniteSession.sql("SELECT TRIM(TRAILING 'B' FROM str) FROM strings WHERE id = 9")

			checkOptimizationResult(df, "SELECT RTRIM(str, 'B') FROM strings WHERE id = 9")

			val data = Tuple1("BAAA")

			checkQueryData(df, data)
		}

        it("LOWER") {
            val df = igniteSession.sql("SELECT LOWER(str) FROM strings WHERE id = 2")

            checkOptimizationResult(df, "SELECT LOWER(str) FROM strings WHERE id = 2")

            val data = Tuple1("aaa")

            checkQueryData(df, data)
        }

        it("UPPER") {
            val df = igniteSession.sql("SELECT UPPER(str) FROM strings WHERE id = 1")

            checkOptimizationResult(df, "SELECT UPPER(str) FROM strings WHERE id = 1")

            val data = Tuple1("AAA")

            checkQueryData(df, data)
        }

        it("LOWER(RTRIM)") {
            val df = igniteSession.sql("SELECT LOWER(RTRIM(str)) FROM strings WHERE id = 3")

            checkOptimizationResult(df, "SELECT LOWER(RTRIM(str)) FROM strings WHERE id = 3")

            val data = Tuple1("aaa")

            checkQueryData(df, data)
        }

        it("LOCATE") {
            val df = igniteSession.sql("SELECT LOCATE('D', str) FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT LOCATE('D', str, 1) FROM strings WHERE id = 6")

            val data = Tuple1(4)

            checkQueryData(df, data)
        }

        it("LOCATE - 2") {
            val df = igniteSession.sql("SELECT LOCATE('A', str) FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT LOCATE('A', str, 1) FROM strings WHERE id = 6")

            val data = Tuple1(1)

            checkQueryData(df, data)
        }

        it("POSITION") {
            val df = igniteSession.sql("SELECT instr(str, 'BCD') FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT POSITION('BCD', str) as \"instr(str, BCD)\" FROM strings " +
                "WHERE id = 6")

            val data = Tuple1(2)

            checkQueryData(df, data)
        }

        it("CONCAT") {
            val df = igniteSession.sql("SELECT concat(str, 'XXX') FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT concat(str, 'XXX') FROM strings WHERE id = 6")

            val data = Tuple1("ABCDEFXXX")

            checkQueryData(df, data)
        }

        it("RPAD") {
            val df = igniteSession.sql("SELECT RPAD(str, 10, 'X') FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT RPAD(str, 10, 'X') FROM strings WHERE id = 6")

            val data = Tuple1("ABCDEFXXXX")

            checkQueryData(df, data)
        }

        it("LPAD") {
            val df = igniteSession.sql("SELECT LPAD(str, 10, 'X') FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT LPAD(str, 10, 'X') FROM strings WHERE id = 6")

            val data = Tuple1("XXXXABCDEF")

            checkQueryData(df, data)
        }

        it("REPEAT") {
            val df = igniteSession.sql("SELECT REPEAT(str, 2) FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT REPEAT(str, 2) FROM strings WHERE id = 6")

            val data = Tuple1("ABCDEFABCDEF")

            checkQueryData(df, data)
        }

        it("SUBSTRING") {
            val df = igniteSession.sql("SELECT SUBSTRING(str, 4, 3) FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT SUBSTR(str, 4, 3) as \"SUBSTRING(str, 4, 3)\" FROM strings " +
                "WHERE id = 6")

            val data = Tuple1("DEF")

            checkQueryData(df, data)
        }

        it("SPACE") {
            val df = igniteSession.sql("SELECT SPACE(LENGTH(str)) FROM strings WHERE id = 1")

            checkOptimizationResult(df, "SELECT SPACE(CAST(LENGTH(str) AS INTEGER)) as \"SPACE(LENGTH(str))\" " +
                "FROM strings WHERE id = 1")

            val data = Tuple1("   ")

            checkQueryData(df, data)
        }

        it("ASCII") {
            val df = igniteSession.sql("SELECT ASCII(str) FROM strings WHERE id = 7")

            checkOptimizationResult(df, "SELECT ASCII(str) FROM strings WHERE id = 7")

            val data = Tuple1(50)

            checkQueryData(df, data)
        }

        it("REGEXP_REPLACE") {
            val df = igniteSession.sql("SELECT REGEXP_REPLACE(str, '(\\\\d+)', 'num') FROM strings WHERE id = 7")

            checkOptimizationResult(df, "SELECT REGEXP_REPLACE(str, '(\\d+)', 'num') FROM strings " +
                "WHERE id = 7")

            val data = Tuple1("num")

            checkQueryData(df, data)
        }

        it("CONCAT_WS") {
            val df = igniteSession.sql("SELECT id, CONCAT_WS(', ', str, 'after') FROM strings " +
                "WHERE id >= 7 AND id <= 8")

            checkOptimizationResult(df, "SELECT id, CONCAT_WS(', ', str, 'after') FROM strings " +
                "WHERE id >= 7 AND id <= 8")

            val data = (
                (7, "222, after"),
                (8, "after"))

            checkQueryData(df, data)
        }

        it("TRANSLATE") {
            val df = igniteSession.sql("SELECT id, TRANSLATE(str, 'DEF', 'ABC') FROM strings WHERE id = 6")

            checkOptimizationResult(df, "SELECT id, TRANSLATE(str, 'DEF', 'ABC') FROM strings " +
                "WHERE id = 6")

            val data = Tuple1((6, "ABCABC"))

            checkQueryData(df, data)
        }
    }

    def createStringTable(client: Ignite, cacheName: String): Unit = {
        val cache = client.cache(cacheName)

        cache.query(new SqlFieldsQuery(
            """
              | CREATE TABLE strings (
              |    id LONG,
              |    str VARCHAR,
              |    PRIMARY KEY (id)) WITH "backups=1"
            """.stripMargin)).getAll

        val qry = new SqlFieldsQuery("INSERT INTO strings (id, str) values (?, ?)")

        cache.query(qry.setArgs(1L.asInstanceOf[JLong], "aaa")).getAll
        cache.query(qry.setArgs(2L.asInstanceOf[JLong], "AAA")).getAll
        cache.query(qry.setArgs(3L.asInstanceOf[JLong], "AAA   ")).getAll
        cache.query(qry.setArgs(4L.asInstanceOf[JLong], "   AAA")).getAll
        cache.query(qry.setArgs(5L.asInstanceOf[JLong], "   AAA   ")).getAll
        cache.query(qry.setArgs(6L.asInstanceOf[JLong], "ABCDEF")).getAll
        cache.query(qry.setArgs(7L.asInstanceOf[JLong], "222")).getAll
        cache.query(qry.setArgs(8L.asInstanceOf[JLong], null)).getAll
        cache.query(qry.setArgs(9L.asInstanceOf[JLong], "BAAAB")).getAll
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createStringTable(client, DEFAULT_CACHE)

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
