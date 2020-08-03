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

import org.apache.ignite.IgniteException
import org.apache.ignite.spark.AbstractDataFrameSpec.{PERSON_TBL_NAME, PERSON_TBL_NAME_2, TEST_CONFIG_FILE}
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.spark.impl.sqlTableInfo
import org.apache.ignite.testframework.GridTestUtils.resolveIgnitePath
import org.apache.spark.sql.SaveMode.{Append, Ignore, Overwrite}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.sql.functions._

/**
  */
@RunWith(classOf[JUnitRunner])
class IgniteSQLDataFrameWriteSpec extends AbstractDataFrameSpec {
    var personDataFrame: DataFrame = _

    describe("Write DataFrame into a Ignite SQL table") {
        it("Save data frame as a new table") {
            val rowsCnt = personDataFrame.count()

            personDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "new_persons")
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .save()

            assert(rowsCnt == rowsCount("new_persons"), "Data should be saved into 'new_persons' table")
        }

        it("Save data frame to existing table") {
            val rowsCnt = personDataFrame.count()

            personDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, PERSON_TBL_NAME_2)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id, city_id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1, affinityKey=city_id")
                .mode(Overwrite)
                .save()

            assert(rowsCnt == rowsCount(PERSON_TBL_NAME_2), s"Data should be saved into $PERSON_TBL_NAME_2 table")
        }

        it("Save data frame to existing table with streamer options") {
            val rowsCnt = personDataFrame.count()

            personDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, PERSON_TBL_NAME_2)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id, city_id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1, affinityKey=city_id")
                .option(OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS, 3)
                .option(OPTION_STREAMER_PER_NODE_BUFFER_SIZE, 1)
                .option(OPTION_STREAMER_FLUSH_FREQUENCY, 10000)
                .mode(Overwrite)
                .save()

            assert(rowsCnt == rowsCount(PERSON_TBL_NAME_2), s"Data should be saved into $PERSON_TBL_NAME_2 table")
        }

        it("Ignore save operation if table exists") {
            //Count of records before saving
            val person2RowsCntBeforeSave = rowsCount(PERSON_TBL_NAME_2)

            personDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, PERSON_TBL_NAME_2)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id, city_id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1, affinityKey=city_id")
                .mode(Ignore)
                .save()

            assert(rowsCount(PERSON_TBL_NAME_2) == person2RowsCntBeforeSave, "Save operation should be ignored.")
        }

        it("Append data frame data to existing table") {
            //Count of records before appending
            val person2RowsCnt = rowsCount(PERSON_TBL_NAME_2)

            //Count of appended records
            val personRowsCnt = personDataFrame.count()

            personDataFrame
                .withColumn("id", col("id") + person2RowsCnt) //Edit id column to prevent duplication
                .write.format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, PERSON_TBL_NAME_2)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id, city_id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "backups=1, affinityKey=city_id")
                .mode(Append)
                .save()

            assert(rowsCount(PERSON_TBL_NAME_2) == person2RowsCnt + personRowsCnt,
                s"Table $PERSON_TBL_NAME_2 should contain data from $PERSON_TBL_NAME")
        }

        it("Save another data source data as a Ignite table") {
            val citiesDataFrame = spark.read.json(
                resolveIgnitePath("modules/spark/src/test/resources/cities.json").getAbsolutePath)

            citiesDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "json_city")
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
                .save()

            assert(rowsCount("json_city") == citiesDataFrame.count(),
                "Table json_city should contain data from json file.")
        }

        it("Save data frame as a new table with save('table_name')") {
            val rowsCnt = personDataFrame.count()

            personDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .save("saved_persons")

            assert(rowsCnt == rowsCount("saved_persons"), "Data should be saved into 'saved_persons' table")
        }

        it("Should keep first row if allowOverwrite is false") {
            val nonUniqueCitiesDataFrame = spark.read.json(
                resolveIgnitePath("modules/spark/src/test/resources/cities_non_unique.json").getAbsolutePath)

            nonUniqueCitiesDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "first_row_json_city")
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
                .option(OPTION_STREAMER_ALLOW_OVERWRITE, false)
                .save()

            val cities = readTable("first_row_json_city").collect().sortBy(_.getAs[Long]("ID"))

            assert(cities(0).getAs[String]("NAME") == "Forest Hill")
            assert(cities(1).getAs[String]("NAME") == "Denver")
            assert(cities(2).getAs[String]("NAME") == "St. Petersburg")
        }

        it("Should keep last row if allowOverwrite is true") {
            val nonUniqueCitiesDataFrame = spark.read.json(
                resolveIgnitePath("modules/spark/src/test/resources/cities_non_unique.json").getAbsolutePath)

            nonUniqueCitiesDataFrame.write
                .format(FORMAT_IGNITE)
                .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                .option(OPTION_TABLE, "last_row_json_city")
                .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                .option(OPTION_CREATE_TABLE_PARAMETERS, "template=replicated")
                .option(OPTION_STREAMER_ALLOW_OVERWRITE, true)
                .save()

            val cities = readTable("last_row_json_city").collect().sortBy(_.getAs[Long]("ID"))

            assert(cities(0).getAs[String]("NAME") == "Paris")
            assert(cities(1).getAs[String]("NAME") == "New York")
            assert(cities(2).getAs[String]("NAME") == "Moscow")
        }
    }

    describe("Wrong DataFrame Write Options") {
        it("Should throw exception with ErrorIfExists for a existing table") {
            intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                    .mode(SaveMode.ErrorIfExists)
                    .save()
            }
        }

        it("Should throw exception if primary key fields not specified") {
            intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, "persons_no_pk")
                    .save()
            }
        }

        it("Should throw exception if primary key fields not specified for existing table") {
            intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .mode(Overwrite)
                    .save()
            }

            val tblInfo = sqlTableInfo(client, PERSON_TBL_NAME, None)

            assert(tblInfo.isDefined, s"Table $PERSON_TBL_NAME should exists.")
        }

        it("Should throw exception for wrong pk field") {
            intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "unknown_field")
                    .mode(Overwrite)
                    .save()
            }

            val tblInfo = sqlTableInfo(client, PERSON_TBL_NAME, None)

            assert(tblInfo.isDefined, s"Table $PERSON_TBL_NAME should exists.")
        }

        it("Should throw exception for wrong pk field - 2") {
            intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id,unknown_field")
                    .mode(Overwrite)
                    .save()
            }

            val tblInfo = sqlTableInfo(client, PERSON_TBL_NAME, None)

            assert(tblInfo.isDefined, s"Table $PERSON_TBL_NAME should exists.")
        }

        it("Should throw exception for wrong WITH clause") {
            intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, "person_unsupported_with")
                    .option(OPTION_CREATE_TABLE_PARAMETERS, "unsupported_with_clause")
                    .mode(Overwrite)
                    .save()
            }
        }

        it("Should throw exception for wrong table name") {
            intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, "wrong-table-name")
                    .option(OPTION_CREATE_TABLE_PARAMETERS, "unsupported_with_clause")
                    .mode(Overwrite)
                    .save()
            }
        }

        it("Should throw exception if streamingFlushFrequency is not a number") {
            intercept[NumberFormatException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                    .option(OPTION_STREAMER_FLUSH_FREQUENCY, "not_a_number")
                    .mode(Overwrite)
                    .save()
            }
        }

        it("Should throw exception if streamingPerNodeBufferSize is not a number") {
            intercept[NumberFormatException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                    .option(OPTION_STREAMER_PER_NODE_BUFFER_SIZE, "not_a_number")
                    .mode(Overwrite)
                    .save()
            }
        }

        it("Should throw exception if streamingPerNodeParallelOperations is not a number") {
            intercept[NumberFormatException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                    .option(OPTION_STREAMER_PER_NODE_PARALLEL_OPERATIONS, "not_a_number")
                    .mode(Overwrite)
                    .save()
            }
        }

        it("Should throw exception if streamerAllowOverwrite is not a boolean") {
            intercept[IllegalArgumentException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, PERSON_TBL_NAME)
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                    .option(OPTION_STREAMER_ALLOW_OVERWRITE, "not_a_boolean")
                    .mode(Overwrite)
                    .save()
            }
        }

        it("Should throw exception if saving data frame as a new table with non-PUBLIC schema") {
            val ex = intercept[IgniteException] {
                personDataFrame.write
                    .format(FORMAT_IGNITE)
                    .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
                    .option(OPTION_TABLE, "nonexistant-table-name")
                    .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, "id")
                    .option(OPTION_SCHEMA, "mySchema")
                    .save()
            }

            assertEquals(ex.getMessage,
                "Creating new tables in schema mySchema is not valid, tables must only be created in " +
                org.apache.ignite.internal.processors.query.QueryUtils.DFLT_SCHEMA)
        }
    }

    override protected def beforeAll(): Unit = {
        super.beforeAll()

        createPersonTable(client, "cache1")

        createPersonTable2(client, "cache1")

        createCityTable(client, "cache1")

        personDataFrame = spark.read
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, PERSON_TBL_NAME)
            .load()

        personDataFrame.createOrReplaceTempView("person")
    }

    /**
      * @param tbl Table name.
      * @return Count of rows in table.
      */
    protected def rowsCount(tbl: String): Long = readTable(tbl).count()

    /**
      * @param tbl Table name.
      * @return Ignite Table DataFrame.
      */
    protected def readTable(tbl: String): DataFrame =
        spark.read
            .format(FORMAT_IGNITE)
            .option(OPTION_CONFIG_FILE, TEST_CONFIG_FILE)
            .option(OPTION_TABLE, tbl)
            .load()
}
