package de.kp.works.ignite.streamer.fiware.table
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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.google.gson.{JsonArray, JsonObject}
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.json.JsonUtil
import de.kp.works.ignite.streamer.fiware.FiwareEvent
import de.kp.works.ignite.transform.fiware.FiwareSchema
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConversions._

object FiwareTransformer {

  protected val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private val fiwareCfg = WorksConf.getCfg(WorksConf.FIWARE_CONF)
  private val fiwareKey = fiwareCfg.getString("primaryKey")

  private val primaryKey = StructField(fiwareKey, StringType, nullable = false)

  def transform(fiwareEvents:Seq[FiwareEvent]): (StructType, Seq[Row]) = {

    try {
      /*
       * Add primary key to Fiware schema
       */
      val fields = Array(primaryKey) ++ FiwareSchema.schema().fields
      val schema = StructType(fields)

      val rows = fiwareEvents.flatMap(fiwareEvent => {

        val service     = fiwareEvent.service
        val servicePath = fiwareEvent.servicePath

        val payload = fiwareEvent.payload
        /*
         * We expect 2 fields, `subscriptionId` and `data`
         */
        val subscription = payload.get("subscriptionId").getAsString
        val entities = payload.get("data").getAsJsonArray

        entities.flatMap(elem => {

          val entity = elem.getAsJsonObject

          val entityId = entity.get("id").getAsString
          val entityType = entity.get("type").getAsString

          val entityCtx =
            if (entity.has("@context"))
              entity.get("@context").getAsJsonArray

            else {

              val context = new JsonArray
              context.add("https://schema.lab.fiware.org/ld/context")

              context

            }

          /*
           * Extract attributes from entity
           */
          val excludes = List("id", "type")
          val attrNames = entity.keySet().filter(attrName => !excludes.contains(attrName))

          attrNames.map(attrName => {
            /*
             * Leverage Jackson mapper to determine the data type of the
             * provided value
             */
            val attrObj = mapper.readValue(entity.get(attrName).toString, classOf[Map[String, Any]])

            val attrType = attrObj.getOrElse("type", "NULL").asInstanceOf[String]
            val attrValu = attrObj.get("value") match {
              case Some(v) => mapper.writeValueAsString(v)
              case _ => ""
            }

            val metadata = attrObj.get("metadata") match {
              case Some(v) => mapper.writeValueAsString(v)
              case _ => ""
            }

            val rowObj = new JsonObject
            rowObj.addProperty("subscription", subscription)

            rowObj.addProperty("service", service)
            rowObj.addProperty("service_path", servicePath)

            rowObj.addProperty("entity_id", entityId)
            rowObj.addProperty("entity_type", entityType)

            rowObj.addProperty("attr_name", attrName)
            rowObj.addProperty("attr_type", attrType)
            rowObj.addProperty("attr_value", attrValu)

            rowObj.addProperty("metadata", metadata)
            rowObj.add("entityCtx", entityCtx)

            JsonUtil.json2Row(rowObj, schema)

          })

        })

      })

      (schema, rows)

    } catch {
      case _: Throwable => (StructType(Array.empty[StructField]), Seq.empty[Row])
    }
  }

}
