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
import org.apache.spark.sql.{DataFrame, Row}

class EdgeRDDWriter(ic:IgniteContext, namespace:String) extends RDDWriter(ic) {

  table = Some(namespace + "_" + IgniteConstants.EDGES)

  /* Build cache configuration */
  cfg = Some(IgniteUtil.createCacheCfg(table.get, ElementType.EDGE, CacheMode.REPLICATED))

  /* Create cache */
  IgniteUtil.createCacheIfNotExists(ic.ignite(), table.get, cfg.get)

  /**
   * This method auto-generates cache keys, i.e. this method
   * appends entries to an already existing edge cache.
   */
  def write(dataframe:DataFrame, keepBinary:Boolean=false):Unit = {

    save(dataframe, (row:Row, ic:IgniteContext) => {

      val valueBuilder = ic.ignite().binary().builder(table.get)
      /*
       * Transform [Row] into an edge-specific [BinaryObject]
       */
      row.schema.fields.foreach(field => {

        val fname = field.name
        val ftype = field.dataType.simpleString

        val index = row.schema.fieldIndex(fname)
        val fvalue = row.get(row.schema.fieldIndex(fname))

        fname match {
          case IgniteConstants.ID_COL_NAME =>
            /*
             * The current implementation supports [Long]
             * and [String] data types for edge identifiers
             */
            if (ftype == "bigint" || ftype == "long") {
              if (fvalue == null)
                valueBuilder.setField[java.lang.Long](fname,null,classOf[java.lang.Long])
              else
                valueBuilder.setField(fname,row.getLong(index))

              valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME, ValueType.LONG.name())
            }
            else if (ftype == "string") {
              if (fvalue == null)
                valueBuilder.setField[String](fname,null,classOf[String])
              else
                valueBuilder.setField(fname,row.getString(index))

              valueBuilder.setField(IgniteConstants.ID_TYPE_COL_NAME, ValueType.STRING.name())
            }
            else
              throw new Exception(s"The data type for $fname is not supported.")

          case IgniteConstants.LABEL_COL_NAME =>
            valueBuilder.setField(fname,row.getString(index))

          case IgniteConstants.TO_COL_NAME =>
            /*
             * The current implementation supports [Long]
             * and [String] data types for vertex identifiers
             */
            if (ftype == "bigint" || ftype == "long") {
              if (fvalue == null)
                valueBuilder.setField[java.lang.Long](fname,null,classOf[java.lang.Long])
              else
                valueBuilder.setField(fname,row.getLong(index))

              valueBuilder.setField(IgniteConstants.TO_TYPE_COL_NAME, ValueType.LONG.name())
            }
            else if (ftype == "string") {
              if (fvalue == null)
                valueBuilder.setField[String](fname,null,classOf[String])
              else
                valueBuilder.setField(fname,row.getString(index))

              valueBuilder.setField(IgniteConstants.TO_TYPE_COL_NAME, ValueType.STRING.name())
            }
            else
              throw new Exception(s"The data type for $fname is not supported.")

          case IgniteConstants.FROM_COL_NAME =>
            /*
             * The current implementation supports [Long]
             * and [String] data types for vertex identifiers
             */
            if (ftype == "bigint" || ftype == "long") {
              if (fvalue == null)
                valueBuilder.setField[java.lang.Long](fname,null,classOf[java.lang.Long])
              else
                valueBuilder.setField(fname,row.getLong(index))

              valueBuilder.setField(IgniteConstants.FROM_TYPE_COL_NAME, ValueType.LONG.name())
            }
            else if (ftype == "string") {
              if (fvalue == null)
                valueBuilder.setField[String](fname,null,classOf[String])
              else
                valueBuilder.setField(fname,row.getString(index))

              valueBuilder.setField(IgniteConstants.FROM_TYPE_COL_NAME, ValueType.STRING.name())
            }
            else
              throw new Exception(s"The data type for $fname is not supported.")
          /*
           * PROPERTIES
           */
          case _ =>

            valueBuilder.setField(IgniteConstants.PROPERTY_KEY_COL_NAME, fname)

            var colType:String  = ""
            var colValue:String = ""

            ftype match {
              case "bigint" =>
                colType = ValueType.LONG.name()
                if (fvalue != null)
                  colValue = row.getLong(index).toString
              case "boolean" =>
                colType = ValueType.BOOLEAN.name()
                if (fvalue != null)
                  colValue = row.getBoolean(index).toString
              case "byte" =>
                colType = ValueType.BYTE.name()
                if (fvalue != null)
                  colValue = row.getByte(index).toString
              case "date" =>
                /*
                 * [java.sql.Date] is converted and represented as [Long]
                 */
                colType = ValueType.LONG.name()
                if (fvalue != null) {
                  val date = row.getDate(index).getTime
                  colValue = date.toString
                }
              case "double" =>
                colType = ValueType.DOUBLE.name()
                if (fvalue != null)
                  colValue = row.getDouble(index).toString
              case "float" =>
                colType = ValueType.FLOAT.name()
                if (fvalue != null)
                  colValue = row.getFloat(index).toString
              case "int" =>
                colType = ValueType.INT.name()
                if (fvalue != null)
                  colValue = row.getInt(index).toString
              case "long" =>
                colType = ValueType.LONG.name()
                if (fvalue != null)
                  colValue = row.getLong(index).toString
              case "short" =>
                colType = ValueType.SHORT.name()
                if (fvalue != null)
                  colValue = row.getShort(index).toString
              case "string" =>
                colType = ValueType.STRING.name()
                if (fvalue != null)
                  colValue = row.getString(index)
              case "timestamp" =>
                /*
                 * [java.sql.Timestamp] is converted and represented as [Long]
                 */
                if (fvalue != null) {
                  val timestamp = row.getTimestamp(index).getTime
                  colValue = timestamp.toString
                }
              case _ => throw new Exception(s"[ERROR] Data type '$ftype' is not supported")
            }

            valueBuilder.setField(IgniteConstants.PROPERTY_TYPE_COL_NAME,  colType)
            valueBuilder.setField(IgniteConstants.PROPERTY_VALUE_COL_NAME, colValue)
        }

      })
      /*
       * Set timestamp fields
       */
      valueBuilder.setField(IgniteConstants.CREATED_AT_COL_NAME, System.currentTimeMillis())
      valueBuilder.setField(IgniteConstants.UPDATED_AT_COL_NAME, System.currentTimeMillis())

      val cacheKey = java.util.UUID.randomUUID().toString
      val cacheValue = valueBuilder.build()

      (cacheKey, cacheValue)

    }, keepBinary)

  }
}
