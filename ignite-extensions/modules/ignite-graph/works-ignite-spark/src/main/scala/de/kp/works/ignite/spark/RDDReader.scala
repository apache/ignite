package de.kp.works.ignite.spark

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

import org.apache.ignite.binary.BinaryObject
import org.apache.ignite.configuration.CacheConfiguration
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql.DataFrame

abstract class RDDReader(ic:IgniteContext) {

  protected var cfg:Option[CacheConfiguration[String,BinaryObject]] = None
  protected var table:Option[String] = None

  def load(fields:Seq[String]):DataFrame = {

    val columns = fields.mkString(",")
    val sql = s"select $columns from $table"

    val rdd = ic.fromCache(cfg.get) //.withKeepBinary[String,BinaryObject]()
    rdd.sql(sql)

  }

}
