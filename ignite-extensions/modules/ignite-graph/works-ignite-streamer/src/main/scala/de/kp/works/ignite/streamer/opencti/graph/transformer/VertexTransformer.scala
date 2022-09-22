package de.kp.works.ignite.streamer.opencti.graph.transformer
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
import de.kp.works.ignite.mutate._
import de.kp.works.ignite.transform.opencti.stix.STIX

import java.util.Date
import scala.collection.mutable

object VertexTransformer extends BaseTransformer {
  /**
   * A STIX object is either a STIX Domain Object (SDO)
   * or a STIX Cyber Observable (SCO)
   */
  def createStixObject(entityId:String, entityType:String, data:Map[String, Any]):
  (Option[Seq[IgnitePut]], Option[Seq[IgnitePut]]) = {
    /*
     * Associated (internal) vertices & edges
     */
    val vertices = mutable.ArrayBuffer.empty[IgnitePut]
    val edges    = mutable.ArrayBuffer.empty[IgnitePut]

    val vertex = initializeVertex(entityId, entityType, "create")
    var filteredData = data

    /*
     * The remaining part of this method distinguishes between fields
     * that describe describe relations and those that carry object
     * properties.
     */

    /** CREATED BY
     *
     * This field contains the reference (identifier) of the Identity
     * object that created the STIX object. When creating a STIX object
     * vertex, this reference is used to also create an associated edge,
     * from (STIX object) to (Identity).
     */
    if (filteredData.contains("created_by_ref")) {
      /*
       * Creator is transformed to an edge
       */
      val e = EdgeTransformer.createCreatedBy(entityId, filteredData)
      if (e.isDefined) edges += e.get
      /*
       * Remove 'created_by_ref' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "created_by_ref"))
    }

    /** EXTERNAL REFERENCES **/

    if (filteredData.contains("external_references")) {
      /*
       * External references are mapped onto vertices
       * and edges
       */
      val (v, e) = EdgeTransformer.createExternalReferences(entityId, filteredData)

      if (v.isDefined) vertices ++= v.get
      if (e.isDefined) edges    ++= e.get

      filteredData = filteredData.filterKeys(k => !(k == "external_references"))
    }

    /** KILL CHAIN PHASES
     *
     * This refers to Attack-Pattern, Indicator, Malware and Tools
     */
    if (filteredData.contains("kill_chain_phases")) {
      /*
       * Kill chain phases are mapped onto vertices
       * and edges
       */
      val (v, e) = EdgeTransformer.createKillChainPhases(entityId, filteredData)

      if (v.isDefined) vertices ++= v.get
      if (e.isDefined) edges    ++= e.get
      /*
       * Remove 'kill_chain_phases' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "kill_chain_phases"))
    }

    /** OBJECT LABELS
     *
     * This field either contains a list of object labels (default)
     * or references to Object Label objects. When creating a STIX
     * object, the label reference is used to create an associated
     * edge. In case of a provided object label value, another vertex
     * containing the label value and an associated edge is created
     * to connect STIX object and respective label.
     *
     * Exposing object labels as vertices enables graph analytics to
     * identify the most (influencing) labels or tags.
     */
    if (filteredData.contains("labels")) {
      /*
       * Object labels are mapped onto vertices and edges
       */
      val (v, e) = EdgeTransformer.createObjectLabels(entityId, filteredData)

      if (v.isDefined) vertices ++= v.get
      if (e.isDefined) edges    ++= e.get
      /*
       * Remove 'labels' from the provided dataset to restrict
       * further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "labels"))

    }

    /** OBJECT MARKINGS
     *
     * This field contains a list of references (identifiers) of
     * Object Markings that are associated with the STIX object.
     *
     * When creating the STIX object also a list of associated
     * edges is created, pointing from the STIX object to the
     * respective Object Marking.
     */
    if (filteredData.contains("object_marking_refs")) {
      /*
       * Object markings are transformed (in contrast to external
       * reference) to edges only
       */
      val e = EdgeTransformer.createObjectMarkings(entityId, filteredData)
      if (e.isDefined) edges ++= e.get
      /*
       * Remove 'object_marking_refs' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "object_marking_refs"))
    }

    /** OBJECT REFERENCES **/

    if (filteredData.contains("object_refs")) {
      /*
       * Object references are transformed (in contrast to external
       * reference) to edges only
       */
      val e = EdgeTransformer.createObjectReferences(entityId, filteredData)
      if (e.isDefined) edges ++= e.get
      /*
       * Remove 'object_refs' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "object_refs"))
    }

    /** HASHES **/

    if (filteredData.contains("hashes")) {
      /*
       * This approach adds all standard hash algorithms as
       * properties to the vertex, irrespective of whether
       * the values are defined or not. This makes update
       * request a lot easier.
       */
      val hashes = transformHashes(data("hashes"))
      STIX.STANDARD_HASHES.foreach(hash => {
        val value = hashes.getOrElse(hash, "")
        vertex.addColumn(hash, "STRING", value)
      })

      filteredData = filteredData.filterKeys(k => !(k == "hashes"))
    }
    /*
     * Add remaining properties to the SDO or SCO; the current
     * implementation accepts properties of a basic data type
     * or a list where the components specify basic data types.
     */
    filteredData.keySet.foreach(propKey => {

      val value = filteredData(propKey)
      value match {
        case values: List[Any] =>
          try {

            val basicType = getBasicType(values.head)
            putValues(propKey, basicType, values, vertex)

          } catch {
            case _:Throwable => /* Do nothing */
          }

        case _ =>
          try {

            val propType = getBasicType(value)
            putValue(propKey, propType, value, vertex)

          } catch {
            case _:Throwable => /* Do nothing */
          }
      }

    })

    vertices += vertex
    (Some(vertices), Some(edges))
  }
  /**
   * This method deletes a [Vertex] object from the respective
   * Ignite cache. Removing selected vertex properties is part
   * of the update implementation (with patch action `remove`)
   */
  def deleteStixObject(entityId:String):(Option[Seq[IgniteDelete]], Option[Seq[IgniteDelete]]) = {
    val delete = new IgniteDelete(entityId, ElementType.VERTEX)
    (None, Some(Seq(delete)))
  }

  def updateStixObject(entityId:String, entityType:String, data:Map[String, Any]):
  (Option[Seq[IgniteMutation]], Option[Seq[IgniteMutation]]) = {
    /*
     * Associated (internal) vertices & edges
     */
    val vertices = mutable.ArrayBuffer.empty[IgniteMutation]
    val edges    = mutable.ArrayBuffer.empty[IgniteMutation]

    val vertex = initializeVertex(entityId, entityType, "update")
     /*
     * Retrieve patch data from data
     */
    val patch = getPatch(data)
    if (patch.isDefined) {

      val patchData = patch.get

      patchData.keySet.foreach(operation => {
        var properties = patchData(operation).asInstanceOf[Map[String, List[Any]]]
        if (properties.contains("created_by_ref")) {
          /**
           * CREATED BY
           *
           * The current implementation expects that the values
           * provided by the patch are identifiers to Identity
           * objects
           */
          val values = properties("created_by_ref")
          values match {
            case references: List[_] =>
              references.foreach(reference => {
                if (operation == "add") {

                  val e = EdgeTransformer.createCreatedBy(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else if (operation == "remove") {
                  /*
                   * Retrieve the [Edge] that refers to the `from`
                   * and `to` operator
                   */
                  val e = EdgeTransformer.deleteCreatedBy(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else {
                  /*
                   * The current implementation does not support
                   * `replace` operations for references
                   */
                }
              })
            case _ =>
              val now = new Date().toString
              throw new Exception(s"[ERROR] $now - The `created_by_ref` patch is not a List[String].")
          }
          /*
           * Remove `created_by_ref` from properties
           */
          properties = properties.filterKeys(k => !(k == "created_by_ref"))
        }
        if (properties.contains("external_references")) {
          /**
           * EXTERNAL REFERENCES
           *
           * The current implementation expects that the values
           * provided by the patch are identifiers to external
           * objects
           */
          val values = properties("external_references")
          values match {
            case references: List[_] =>
              references.foreach(reference => {
                if (operation == "add") {

                  val e = EdgeTransformer.createExternalReference(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else if (operation == "remove") {
                  /*
                     * Retrieve the [Edge] that refers to the `from`
                     * and `to` operator
                     */
                  val e = EdgeTransformer.deleteExternalReference(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else {
                  /*
                   * The current implementation does not support
                   * `replace` operations for references
                   */
                }
              })
            case _ =>
              val now = new Date().toString
              throw new Exception(s"[ERROR] $now - The `external_references` patch is not a List[String].")
          }
          /*
           * Remove `external_references` from properties
           */
          properties = properties.filterKeys(k => !(k == "external_references"))
        }
        if (properties.contains("kill_chain_phases")) {
          /**
           * KILL CHAIN PHASES
           *
           * The current implementation expects that the values
           * provided by the patch are identifiers to external
           * kill chain objects
           */
          val values = properties("kill_chain_phases")
          values match {
            case references: List[_] =>
              references.foreach(reference => {
                if (operation == "add") {

                  val e = EdgeTransformer.createKillChainPhase(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else if (operation == "remove") {
                  /*
                     * Retrieve the [Edge] that refers to the `from`
                     * and `to` operator
                     */
                  val e = EdgeTransformer.deleteKillChainPhase(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else {
                  /*
                     * The current implementation does not support
                     * `replace` operations for references
                     */
                }
              })
            case _ =>
              val now = new Date().toString
              throw new Exception(s"[ERROR] $now - The `kill_chain_phases` patch is not a List[String].")
          }
          /*
           * Remove `kill_chain_phases` from properties
           */
          properties = properties.filterKeys(k => !(k == "kill_chain_phases"))
        }
        if (properties.contains("labels")) {
          /**
           * OBJECT LABELS
           *
           * The current implementation expects that the values
           * provided by the patch are identifiers to external
           * object label objects
           */
          val values = properties("labels")
          values match {
            case references: List[_] =>
              references.foreach(reference => {
                if (operation == "add") {

                  val e = EdgeTransformer.createObjectLabel(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else if (operation == "remove") {
                  /*
                     * Retrieve the [Edge] that refers to the `from`
                     * and `to` operator
                     */
                  val e = EdgeTransformer.deleteObjectLabel(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else {
                  /*
                   * The current implementation does not support
                   * `replace` operations for references
                   */
                }
              })
            case _ =>
              val now = new Date().toString
              throw new Exception(s"[ERROR] $now - The `labels` patch is not a List[String].")
          }
          /*
           * Remove `labels` from properties
           */
          properties = properties.filterKeys(k => !(k == "labels"))
        }
        if (properties.contains("object_marking_refs")) {
          /**
           * OBJECT MARKINGS
           *
           * The current implementation expects that the values
           * provided by the patch are identifiers to external
           * object marking objects
           */
          val values = properties("object_marking_refs")
          values match {
            case references: List[_] =>
              references.foreach(reference => {
                if (operation == "add") {

                  val e = EdgeTransformer.createObjectMarking(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else if (operation == "remove") {
                  /*
                     * Retrieve the [Edge] that refers to the `from`
                     * and `to` operator
                     */
                  val e = EdgeTransformer.deleteObjectMarking(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else {
                  /*
                   * The current implementation does not support
                   * `replace` operations for references
                   */
                }
              })
            case _ =>
              val now = new Date().toString
              throw new Exception(s"[ERROR] $now - The `object_marking_refs` patch is not a List[String].")
          }
          /*
           * Remove `object_marking_refs` from properties
           */
          properties = properties.filterKeys(k => !(k == "object_marking_refs"))
        }
        if (properties.contains("object_refs")) {
          /**
           * OBJECT REFERENCES
           *
           * The current implementation expects that the values
           * provided by the patch are identifiers to external
           * object references
           */
          val values = properties("object_refs")
          values match {
            case references: List[_] =>
              references.foreach(reference => {
                if (operation == "add") {

                  val e = EdgeTransformer.createObjectReference(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else if (operation == "remove") {
                  /*
                     * Retrieve the [Edge] that refers to the `from`
                     * and `to` operator
                     */
                  val e = EdgeTransformer.deleteObjectReference(entityId, reference.asInstanceOf[String])
                  if (e.isDefined) edges += e.get

                } else {
                  /*
                   * The current implementation does not support
                   * `replace` operations for references
                   */
                }
              })
            case _ =>
              val now = new Date().toString
              throw new Exception(s"[ERROR] $now - The `object_refs` patch is not a List[String].")
          }
          /*
           * Remove `object_refs` from properties
           */
          properties = properties.filterKeys(k => !(k == "object_refs"))
        }
        if (properties.contains("hashes")) {
          /**
           * HASHES
           *
           * This implementation expects that `hashes` is an attribute
           * name within a certain patch; relevant, however, are the
           * hash values associated with the hash algorithm.
           */
          val values = properties("hashes")
          values.head match {
            case _: Map[_,_] =>
              val hashes = values.asInstanceOf[List[Map[String,String]]]
              /*
               * We expect the hash properties as [Map] with fields
               * `algorithm` and `hash`.
               */
              hashes.foreach(hash => {

                if (operation == "add" || operation == "replace") {
                  vertex.addColumn(hash("algorithm"), "STRING", hash("hash"))
                }
                else
                  vertex.addColumn(hash("algorithm"), "STRING", "")
              })
            case _ =>
              val now = new Date().toString
              throw new Exception(s"[ERROR] $now - The `hashes` patch is not a List[Map[String,String].")
          }
          /*
           * Remove `hashes` from properties
           */
          properties = properties.filterKeys(k => !(k == "hashes"))
        }
        /*
         * Update remaining properties of the SDO or SCO
         */
        properties.keySet.foreach(propKey => {
          val values = properties(propKey)
          /*
           * The patch mechanism represents all attribute values
           * as [List] even there is only a single value specified
           */
          if (values.size == 1) {
            val value = values.head
            value match {
              case _: List[Any] =>

                val propType = getBasicType(value)
                if (operation == "add" || operation == "replace") {
                  putValues(propKey, propType, values, vertex)
                }
                else {
                  /*
                   * In this case a delete operation must be defined
                   */
                  val delete = new IgniteDelete(entityId, ElementType.VERTEX)
                  delete.addColumn(propKey)

                  vertices += delete
                }
              case _ =>
                try {

                  val propType = getBasicType(value)
                  if (operation == "add" || operation == "replace") {
                    putValue(propKey, propType, value, vertex)
                  }
                  else {
                    /*
                     * In this case a delete operation must be defined
                     */
                    val delete = new IgniteDelete(entityId, ElementType.VERTEX)
                    delete.addColumn(propKey)

                    vertices += delete
                  }

                } catch {
                  case _:Throwable => /* Do nothing */
                }

            }
          } else {
            /*
             * This is genuine [List] of values: it is expected that
             * this list contains basic values only
             */
            try {

              val basicType = getBasicType(values.head)

              if (operation == "add" || operation == "replace") {
                putValues(propKey, basicType, values, vertex)
              }
              else
                putValues(propKey, basicType, List.empty[Any], vertex)

            } catch {
              case _:Throwable => /* Do nothing */
            }

          }
        })

      })

      /*
       * Finally append the [Vertex] representing the STIX
       * object to the list of vertices
       */
      vertices += vertex
      (Some(vertices), Some(edges))

    }
    else {
      (None, None)
    }
  }

}
