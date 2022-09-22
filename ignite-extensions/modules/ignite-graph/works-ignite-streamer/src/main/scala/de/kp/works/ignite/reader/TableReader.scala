package de.kp.works.ignite.reader

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.IgniteConf
import org.apache.ignite.spark.IgniteDataFrameSettings._
import org.apache.ignite.{Ignite, Ignition}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class TableReader(ignite:Ignite) {

  def read(table:String, sql:String, session:SparkSession):DataFrame = {
    /*
     * An Apache Ignite node should be started already,
     * however, in case this did not happen, we start
     * it here anyhow.
     */
    if (ignite == null)
      Ignition.getOrStart(IgniteConf.fromConfig)

    val dataset = session.read
      .format(FORMAT_IGNITE)
      /*
       * The Apache dataframe write requires the
       * Ignite configuration as a reference to
       * the respective configuration file.
       */
      .option(OPTION_CONFIG_FILE, IgniteConf.fromFile)
      .option(OPTION_TABLE, table)
      .load()

    dataset.createOrReplaceTempView(table)

    session.sql(sql)

  }
}
