package de.kp.works.ignite.writer

/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import de.kp.works.ignite.{IgniteConf, IgniteConnect}
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.spark.sql.{DataFrame, SaveMode}

abstract class TableWriter(connect:IgniteConnect) {
  /**
   * Zeek log events ship with ArrayType(LongType|StringType) fields,
   * but Apache Ignite currently does not support this data type.
   *
   * See: https://issues.apache.org/jira/browse/IGNITE-9229
   *
   * Therefore, we intercept the generated dataframe here and serialize
   * all ArrayType fields before writing to Apache Ignite
   */
  def serializeArrays(input:DataFrame):DataFrame = ???

  /**
   * This method write the provided dataframe to
   * an Apache Ignite cache
   */
  def save(table:String, primaryKey:String, parameters:String, dataframe:DataFrame, saveMode:SaveMode):Unit = {
    /*
     * An Apache Ignite node should be started already,
     * however, in case this did not happen, we start
     * it here anyhow.
     */
    connect.getOrCreate

    val writer = dataframe.write
      /*
       * Assign the Apache Ignite `format` to the
       * dataframe writer
       */
      .format(FORMAT_IGNITE)
      /*
       * The Apache dataframe write requires the
       * Ignite configuration as a reference to
       * the respective configuration file.
       */
      .option(OPTION_CONFIG_FILE, IgniteConf.fromFile)
      .option(OPTION_TABLE, table)
      /*
       * The field name of the primary key is required
       * to enable this Apache dataframe writer
       */
      .option(OPTION_CREATE_TABLE_PRIMARY_KEY_FIELDS, primaryKey)
      .option(OPTION_CREATE_TABLE_PARAMETERS, parameters)
      /*
       * Automatic flush frequency in milliseconds. Essentially, this is
       * the time after which the streamer will make an attempt to submit
       * all data added so far to remote nodes. The flush frequency is
       * defined in milliseconds.
       *
      */
      .option(OPTION_STREAMER_FLUSH_FREQUENCY, 100)
      /*
       * Cache entries that refer to the same primary key are allowed
       * to be overwritten by the Apache Ignite streamer
       */
      .option(OPTION_STREAMER_ALLOW_OVERWRITE, value = true)
      .mode(saveMode)

    writer.save()

  }
}
