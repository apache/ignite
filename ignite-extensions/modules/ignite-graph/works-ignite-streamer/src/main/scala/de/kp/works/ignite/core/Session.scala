package de.kp.works.ignite.core

/**
 * Copyright (c) 2021 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.conf.WorksConf
import org.apache.spark.sql.SparkSession

object Session {

  private val session: SparkSession = initialize

  def initialize: SparkSession = {
    /*
     * Initialize the overall configuration
     * with the internal configuration file
     */
    WorksConf.init()
    /*
     * Extract internal Spark configuration
     */
    val sparkCfg = WorksConf.getSparkCfg
    val driverCfg = sparkCfg.getConfig("driver")

    val driver_maxResultSize = driverCfg.getString("maxResultSize")
    val driver_memory = driverCfg.getString("memory")

    val executorCfg = sparkCfg.getConfig("executor")

    val executor_instances = executorCfg.getString("instances")
    val executor_memory = executorCfg.getString("memory")

    val spark: SparkSession = SparkSession.builder()
      .appName(sparkCfg.getString("appName"))
      .master(sparkCfg.getString("master"))
      /*
       * Driver & executor configuration
       */
      .config("spark.driver.maxResultSize", driver_maxResultSize)
      .config("spark.driver.memory", driver_memory)
      .config("spark.executor.memory", executor_memory)
      .config("spark.executor.instances", executor_instances)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    spark

  }

  def getSession: SparkSession = session

}
