package de.kp.works.ignite.streamer.opencti.table
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
import com.google.gson.JsonArray
import de.kp.works.ignite.json.JsonUtil
import de.kp.works.ignite.sse.SseEvent
import de.kp.works.ignite.transform.opencti.stix.STIX
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
/**
 * The [CTITransformer] is implemented to transform STIX
 * object and sightings. Relations are currently not supported.
 */
object CTITransformer {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  private val emptyBatch  = new JsonArray
  private val emptySchema = StructType(Array.empty[StructField])

  def transform(sseEvents: Seq[SseEvent]): Seq[(String, StructType, Seq[Row])] = {

    try {

      sseEvents.map(sseEvent => {
        /*
         * The event type specifies the data operation
         * associated with the event; this implementation
         * currently supports `create`, `delete` and `update`
         * operations
         */
        val event = sseEvent.eventType
        /*
         * The current implementation leverages the STIX data
         * as `stixData` for further processing
         */
        val stixData = {
          val deserialized = mapper.readValue(sseEvent.data, classOf[Map[String, Any]])

          if (deserialized.contains("data"))
            deserialized("data").asInstanceOf[Map[String, Any]]
          else
            Map.empty[String, Any]

        }

        val entityId = stixData.getOrElse("id", "").asInstanceOf[String]
        val entityType = stixData.getOrElse("type", "").asInstanceOf[String]

        if (entityId.isEmpty || entityType.isEmpty || stixData.isEmpty) {
          (entityType.toLowerCase, StructType(Array.empty[StructField]) , Seq.empty[Row])

        }
        else {

          val filter = Seq("id", "type")
          val data = stixData.filterKeys(k => !filter.contains(k))

          val (schema, batch) = event match {
            case "create" =>
              transformCreate(entityId, entityType, data:Map[String, Any])
            case "update" =>
              transformUpdate(entityId, entityType, data:Map[String, Any])
            case "delete" | "merge" | "sync" => (emptySchema, emptyBatch)
            case _ =>
              val now = new java.util.Date().toString
              throw new Exception(s"[ERROR] $now - Unknown event type detected: $event")
          }
          if (batch.isEmpty)
            (entityType.toLowerCase, schema, Seq.empty[Row])

          else {

            val rows = batch.map(batchElem => {

              val batchObj = batchElem.getAsJsonObject
              JsonUtil.json2Row(batchObj, schema)

            }).toSeq

            (entityType.toLowerCase, schema, rows)

          }

        }

      })
      /*
       * Remove those entity types that are not specified
       * or empty
       */
      .filter{case(entityType, _, _) => entityType.nonEmpty}

    } catch {
      case _: Throwable => Seq.empty[(String, StructType, Seq[Row])]
    }
  }

  private def transformCreate(entityId: String, entityType: String, data: Map[String, Any]):(StructType, JsonArray) = {

    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {
      if (STIX.isStixRelationship(entityType.toLowerCase)) {
        /*
         * The current implementation does not support
         * STIX relationships; it is up to the user to
         * build relations between Ignite caches.
         */
        return  (emptySchema, emptyBatch)
      }

      if (STIX.isStixSighting(entityType.toLowerCase)) {
        return CTIUtil.createSighting(entityId, entityType, data)
      }
      /*
       * The creation of STIX observable and meta relationships
       * should not happen as OpenCTI delegates them to updates
       */
      if (STIX.isStixObservableRelationship(entityType.toLowerCase)) {
        /*
         * The current implementation does not support
         * STIX observable relationships; it is up to
         * the user to build relations between Ignite
         * caches.
         */
        return  (emptySchema, emptyBatch)
      }

      if (STIX.isStixMetaRelationship(entityType.toLowerCase)) {
        /*
         * The current implementation does not support
         * STIX metadata relationships; it is up to
         * the user to build relations between Ignite
         * caches.
         */
        return  (emptySchema, emptyBatch)
      }

      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Unknown relation type detected.")
    }
    else {
      /*
       * A STIX object is either a STIX Domain Object
       * or a STIX Cyber Observable
       */
      CTIUtil.createStixObject(entityId, entityType, data)
    }

  }

  private def transformUpdate(entityId: String, entityType: String, data: Map[String, Any]):(StructType, JsonArray) = {

    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {
      if (STIX.isStixRelationship(entityType.toLowerCase)) {
        /*
         * The current implementation does not support
         * STIX relationships; it is up to the user to
         * build relations between Ignite caches.
         */
        return (emptySchema, emptyBatch)
      }

      if (STIX.isStixSighting(entityType.toLowerCase)) {
        return CTIUtil.updateSighting(entityId, entityType, data)
      }
      /*
       * The creation of STIX observable and meta relationships
       * should not happen as OpenCTI delegates them to updates
       */
      if (STIX.isStixObservableRelationship(entityType.toLowerCase)) {
        /*
         * The current implementation does not support
         * STIX observable relationships; it is up to
         * the user to build relations between Ignite
         * caches.
         */
        return (emptySchema, emptyBatch)
      }

      if (STIX.isStixMetaRelationship(entityType.toLowerCase)) {
        /*
         * The current implementation does not support
         * STIX metadata relationships; it is up to
         * the user to build relations between Ignite
         * caches.
         */
        return (emptySchema, emptyBatch)
      }

      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Unknown relation type detected.")
    }
    else {
      /*
       * A STIX object is either a STIX Domain Object
       * or a STIX Cyber Observable
       */
      CTIUtil.updateStixObject(entityId, entityType, data)
    }

  }

}
