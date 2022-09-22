package de.kp.works.ignite.streamer.fiware.graph

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

import de.kp.works.ignite.mutate.IgnitePut
import de.kp.works.ignite.streamer.fiware.FiwareEvent
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

case class NotificationLD(
   /* Time in milliseconds the notification was received */
   receivedAt: Long,
   /* Fiware service header */
   service: String ,
   /* Fiware service path header */
   servicePath: String,
   /* List of entities */
   entities: Seq[EntityLD] ) extends Serializable

case class EntityLD(
   entityId: String,
   entityType: String,
   attrs: Map[String, Map[String,Any]],
   context: Any) extends Serializable

case class Notification(
   /* Time in milliseconds the notification was received */
   receivedAt: Long,
   /* Fiware service header */
   service: String,
   /* Fiware service path header */
   servicePath: String,
   /* List of entities */
   entities: Seq[Entity] ) extends Serializable

case class Entity(
   entityId: String,
   entityType: String,
   attrs: Map[String, Attribute]) extends Serializable

case class Attribute(
   attrType: Any,
   attrValue: Any,
   metadata:Any ) extends Serializable

trait FiwareTransformer {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def transformNotification(notification:FiwareEvent):(Seq[IgnitePut], Seq[IgnitePut])

  def transform(notifications:Seq[FiwareEvent]):(Seq[IgnitePut], Seq[IgnitePut]) = {

    var edges = Seq.empty[IgnitePut]
    var vertices = Seq.empty[IgnitePut]

    notifications.foreach(notification => {

      val (v, e) = transformNotification(notification)

      vertices = vertices ++ v
      edges = edges ++ e

    })

    (vertices, edges)

  }

  protected def toNgsi(notification:FiwareEvent):Notification = {

    val service = notification.service
    val servicePath = notification.servicePath

    val payload = parse(notification.payload.toString)
      .extract[Map[String,Any]]

    val data = payload("data").asInstanceOf[List[Map[String,Any]]]
    val entities = data.map(item => {

      val entityId = item("id").asInstanceOf[String]
      val entityType = item("type").asInstanceOf[String]

      val keys = item.keySet
        .filter(key => key != "id" & key != "type")

      val attributes = keys.map(k => {
        val m = item(k).asInstanceOf[Map[String,Any]]
        val v = Attribute(m.get("type").orNull, m.get("value").orNull, m.get("metadata").orNull)
        (k,v)
      }).toMap

      Entity(entityId, entityType, attributes)
    })

    Notification(
      receivedAt = System.currentTimeMillis,
      service = service,
      servicePath = servicePath,
      entities = entities
    )

  }

  protected def toNgsiLD(notification:FiwareEvent):NotificationLD = {

    val service = notification.service
    val servicePath = notification.servicePath

    val payload = parse(notification.payload.toString)
      .extract[Map[String,Any]]

    val data = payload("data").asInstanceOf[List[Map[String,Any]]]
    val entities = data.map(item => {

      val entityId = item("id").asInstanceOf[String]
      val entityType = item("type").asInstanceOf[String]

      val context = {
        if (!item.contains("@context"))
          List("https://schema.lab.fiware.org/ld/context")

        else
          item("@context").asInstanceOf[List[String]]
      }

      val keys = item.keySet
        .filter(key => key != "id" & key != "type" & key != "@context")

      val attributes = keys.map(k => {
        val v = item(k).asInstanceOf[Map[String,Any]]
        (k,v)
      }).toMap

      EntityLD(entityId, entityType, attributes, context)

    })

    NotificationLD(
      receivedAt = System.currentTimeMillis,
      service = service,
      servicePath = servicePath,
      entities = entities
    )

  }
}
