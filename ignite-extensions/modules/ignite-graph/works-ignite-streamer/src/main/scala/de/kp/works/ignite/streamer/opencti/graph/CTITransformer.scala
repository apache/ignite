package de.kp.works.ignite.streamer.opencti.graph
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
import de.kp.works.ignite.mutate.{IgniteDelete, IgniteMutation, IgnitePut}
import de.kp.works.ignite.sse.SseEvent
import de.kp.works.ignite.streamer.opencti.graph.transformer.{EdgeTransformer, VertexTransformer}
import de.kp.works.ignite.transform.opencti.stix.STIX

import scala.collection.mutable

object CTITransformer {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * Events format :: SseEvent(id, event, data)
   *
   * The events published by OpenCTI are based on the STIX format:
   *
   * id: {Event stream id} -> Like 1620249512318-0
   * event: {Event type} -> create / update / delete
   * data: { -> The complete event data
   * markings: [] -> Array of markings IDS of the element
   * origin: {Data Origin} -> Complex object with different information about the origin of the event
   * data: {STIX data} -> The STIX representation of the data.
   * message -> A simple string to easy understand the event
   * version -> The version number of the event
   * }
   */
  def transform(sseEvents: Seq[SseEvent]): (Seq[IgniteMutation], Seq[IgniteMutation]) = {

    val vertices = mutable.ArrayBuffer.empty[IgniteMutation]
    val edges = mutable.ArrayBuffer.empty[IgniteMutation]

    sseEvents.foreach(sseEvent => {
      /*
       * The event type specifies the data operation
       * associated with the event; this implementation
       * currently supports `create`, `delete` and `update`
       * operations
       */
      val ctiEvent = sseEvent.eventType
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
        /* Do nothing */
      }
      else {

        val filter = Seq("id", "type")
        val data = stixData.filterKeys(k => !filter.contains(k))

        val (v, e) = ctiEvent match {
          case "create" =>
            transformCreate(entityId, entityType, data)
          case "delete" =>
            transformDelete(entityId, entityType, data)
          case "merge" | "sync" => (None, None)
          case "update" =>
            transformUpdate(entityId, entityType, data)
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - Unknown event type detected: $ctiEvent")
        }

        if (v.isDefined) vertices ++= v.get
        if (e.isDefined) edges ++= e.get

      }

    })

    (vertices, edges)

  }

  private def transformCreate(entityId: String, entityType: String, data: Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * The current implementation takes non-edges as nodes;
     * an edge can a `relationship` or `sighting`, and also
     * a meta and cyber observable relationship
     */
    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {
      if (STIX.isStixRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.createRelationship(entityId, entityType, data)
      }

      if (STIX.isStixSighting(entityType.toLowerCase)) {
        return EdgeTransformer.createSighting(entityId, entityType, data)
      }
      /*
       * The creation of STIX observable and meta relationships
       * should not happen as OpenCTI delegates them to updates
       */
      if (STIX.isStixObservableRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.createObservableRelationship(entityId, entityType, data)
      }

      if (STIX.isStixMetaRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.createMetaRelationship(entityId, entityType, data)
      }

      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Unknown relation type detected.")
    }
    else {
      /*
       * A STIX object is either a STIX Domain Object
       * or a STIX Cyber Observable
       */
      VertexTransformer.createStixObject(entityId, entityType, data)
    }

  }

  private def transformDelete(entityId: String, entityType: String, data: Map[String, Any]):
  (Option[Seq[IgniteDelete]], Option[Seq[IgniteDelete]]) = {
    /*
     * The current implementation takes non-edges as nodes;
     * an edge can a `relationship` or `sighting`, and also
     * a meta and cyber observable relationship
     */
    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {

      if (STIX.isStixRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.deleteRelationship(entityId)
      }

      if (STIX.isStixSighting(entityType.toLowerCase)) {
        return EdgeTransformer.deleteSighting(entityId)
      }

      if (STIX.isStixObservableRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.deleteMetaRelationship(entityId)
      }

      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Unknown relation type detected.")
    }
    else {
      VertexTransformer.deleteStixObject(entityId)
    }
  }

  /**
   * Create & update request result in a list of [IgnitePut], but
   * must be processed completely different as OpenCTI leverages
   * a complex `x_opencti_patch` field to specify updates.
   */
  private def transformUpdate(entityId: String, entityType: String, data: Map[String, Any]):
  (Option[Seq[IgniteMutation]], Option[Seq[IgniteMutation]]) = {
    /*
     * The current implementation takes non-edges as nodes;
     * an edge can a `relationship` or `sighting`, and also
     * a meta and cyber observable relationship
     */
    val isEdge = STIX.isStixEdge(entityType.toLowerCase)
    if (isEdge) {
      if (STIX.isStixRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.updateRelationship(entityId, entityType, data)
      }

      if (STIX.isStixSighting(entityType.toLowerCase)) {
        return EdgeTransformer.updateSighting(entityId, entityType, data)
      }
      /*
       * The creation of STIX observable and meta relationships
       * should not happen as OpenCTI delegates them to updates
       */
      if (STIX.isStixObservableRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.updateObservableRelationship(entityId, entityType, data)
      }

      if (STIX.isStixMetaRelationship(entityType.toLowerCase)) {
        return EdgeTransformer.updateMetaRelationship(entityId, entityType, data)
      }

      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Unknown relation type detected.")
    }
    else {
      /*
       * A STIX object is either a STIX Domain Object
       * or a STIX Cyber Observable
       */
      VertexTransformer.updateStixObject(entityId, entityType, data)
    }
  }
}
