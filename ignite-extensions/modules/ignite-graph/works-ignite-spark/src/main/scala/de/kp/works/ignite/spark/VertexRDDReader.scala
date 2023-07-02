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

import de.kp.works.ignite.graph.ElementType
import de.kp.works.ignite.{IgniteConstants, IgniteUtil, ValueType}
import org.apache.ignite.cache.CacheMode
import org.apache.ignite.spark.IgniteContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, collect_list, struct, udf}

import scala.collection.mutable.ArrayBuffer
/**
 * [VertexRDDReader] retrieves all vertices of a
 * certain graph namespace and transforms them
 * into a GraphFrame-like format.
 */
class VertexRDDReader(ic:IgniteContext, namespace:String) extends RDDReader(ic) {

  table = Some(namespace + "_" + IgniteConstants.VERTICES)

  /* Build cache configuration */
  cfg = Some(IgniteUtil.createCacheCfg(table.get, ElementType.VERTEX, CacheMode.REPLICATED))

  def vertices():DataFrame = {

    val dataframe = load(getFields)
    /*
     * The dataframe contains the the cache entries and
     * we want to transform them into a vertex compliant
     * format
     */
    val aggCols = Seq(
      IgniteConstants.PROPERTY_KEY_COL_NAME,
      IgniteConstants.PROPERTY_TYPE_COL_NAME,
      IgniteConstants.PROPERTY_VALUE_COL_NAME)

    val groupCols = Seq(
      IgniteConstants.ID_COL_NAME,
      IgniteConstants.ID_TYPE_COL_NAME,
      IgniteConstants.LABEL_COL_NAME,
      IgniteConstants.CREATED_AT_COL_NAME,
      IgniteConstants.UPDATED_AT_COL_NAME)

    val aggStruct = struct(aggCols.map(col): _*)
    var output = dataframe
      .groupBy(groupCols.map(col): _*)
      .agg(collect_list(aggStruct).as("properties"))
    /*
     * As a final step, the `id` column is transformed
     * into the right data type
     */
    val idType = output
      .select(IgniteConstants.ID_TYPE_COL_NAME)
      .head.getAs[String](0)

    val toLong = udf((id:String) => id.toLong)
    if (idType == ValueType.LONG.name()) {
      output = output.withColumn(IgniteConstants.ID_COL_NAME, toLong(col(IgniteConstants.ID_COL_NAME)))
    }

    output.drop(IgniteConstants.ID_TYPE_COL_NAME)
  }

  private def getFields:Seq[String] = {

    val fields = ArrayBuffer.empty[String]
    /*
     * The vertex identifier used by TinkerPop to identify
     * an equivalent of a data row
     */
    fields += IgniteConstants.ID_COL_NAME
    /*
     * The vertex identifier type to reconstruct the
     * respective value. IgniteGraph supports [Long]
     * as well as [String] as identifier.
     */
    fields += IgniteConstants.ID_TYPE_COL_NAME
    /*
     * The vertex label used by TinkerPop and IgniteGraph
     */
    fields += IgniteConstants.LABEL_COL_NAME
    /*
     * The timestamp this cache entry has been created.
     */
    fields += IgniteConstants.CREATED_AT_COL_NAME
    /*
     * The timestamp this cache entry has been updated.
     */
    fields += IgniteConstants.UPDATED_AT_COL_NAME
    /*
     * The property section of this cache entry
     */
    fields += IgniteConstants.PROPERTY_KEY_COL_NAME
    fields += IgniteConstants.PROPERTY_TYPE_COL_NAME
    /*
     * The serialized property value
     */
    fields += IgniteConstants.PROPERTY_VALUE_COL_NAME
    /*
     * The [ByteBuffer] representation for the
     * property value is an internal field and
     * not exposed to queries
     */
    fields

  }
}
