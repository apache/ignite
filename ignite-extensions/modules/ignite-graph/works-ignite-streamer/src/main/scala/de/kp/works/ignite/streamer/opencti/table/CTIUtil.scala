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

import com.google.gson.{JsonArray, JsonObject}
import de.kp.works.ignite.conf.WorksConf
import de.kp.works.ignite.transform.opencti.CTISchema
import de.kp.works.ignite.transform.opencti.stix.STIX
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import java.util.Date

object CTIUtil extends BaseUtil {

  private val ctiCfg = WorksConf.getCfg(WorksConf.OPENCTI_CONF)
  private val ctiKey = ctiCfg.getString("primaryKey")

  private val primaryKey = StructField(ctiKey, StringType, nullable = false)

  private val emptyBatch  = new JsonArray
  private val emptySchema = StructType(Array.empty[StructField])

  /** CREATE SUPPORT **/

  def createSighting(entityId: String, entityType: String, data: Map[String, Any]):(StructType, JsonArray) = {

    var filteredData = data
    val batch = new JsonArray

    var sightingRef:String = "NULL"
    var sightedRefs:String = "NULL"
    /**
     *
     * The reference, sighting_of_ref, contains the ID of the SDO
     * that was sighted, which e.g. can be an indicator or cyber
     * observable.
     */
    if (filteredData.contains("sighting_of_ref"))
      sightingRef = data("sighting_of_ref").asInstanceOf[String]

    else if (filteredData.contains("x_opencti_sighting_of_ref"))
      sightingRef = filteredData("x_opencti_sighting_of_ref").asInstanceOf[String]

    else {
      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Sighting does not contain a `sighting_of_ref` identifier.")
    }

    if (filteredData.contains("where_sighted_refs")) {
      /*
       * OpenCTI specifies this field as a single list element
       */
      val ids = filteredData("where_sighted_refs").asInstanceOf[List[String]]
      if (ids.isEmpty) {
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Sighting does not contain a `where_sighted_refs` identifier.")
      }
      else
        sightedRefs = ids.head
    }
    else {
      val now = new java.util.Date().toString
      throw new Exception(s"[ERROR] $now - Sighting does not contain a `where_sighted_refs` identifier.")
    }
    val filter = Seq(
      "sighting_of_ref",
      "where_sighted_refs",
      "x_opencti_sighting_of_ref",
      "x_opencti_where_sighted_refs")

    filteredData = filteredData.filterKeys(k => !filter.contains(k))
    /*
     * The following sighting attributes are added as properties
     */
    val properties = Seq(
      "attribute_count",
      "confidence",
      "count",
      "created",
      "created_by_ref",
      "description",
      "first_seen",
      "last_seen",
      "modified",
      "name")

    properties.foreach(propKey => {

      val batchObj = new JsonObject
      batchObj.addProperty("action", "create")
      batchObj.addProperty("operation", "add")

      batchObj.addProperty("entity_id", entityId)
      batchObj.addProperty("entity_type", entityType)

      batchObj.addProperty("sighting_of_ref", sightingRef)
      batchObj.addProperty("where_sighted_refs", sightedRefs)

      val (propType, propValu) = propKey match {
        case "attribute_count" | "count" | "confidence" =>
          ("INT", filteredData.getOrElse(propKey, 0).asInstanceOf[Int].toString)
        case _ =>
          ("STRING", filteredData.getOrElse(propKey, "").asInstanceOf[String])
      }

      batchObj.addProperty("attr_name", propKey)
      batchObj.addProperty("attr_type", propType)

      batchObj.addProperty("attr_value", propValu)

      batch.add(batchObj)

    })

    val fields = Array(primaryKey) ++ CTISchema.sighting().fields
    val schema = StructType(fields)

    (schema, batch)

  }
  /**
   * This method transforms a STIX Object into a [JsonArray];
   * as the provided object properties are varying, the object
   * is resolved on the property level, i.e. each property is
   * described as a single JsonObject
   */
  def createStixObject(entityId: String, entityType: String, data: Map[String, Any]):(StructType, JsonArray) = {
    /*
     * Extract other attributes from the provided payload.

     * Basic fields are:
     * - id
     * - type
     *
     * - hashes
     * - source_ref
     * - spec_version
     * - start_time
     * - stop_time
     * - target_ref
     * - x_opencti_id
     * - x_opencti_source_ref
     * - x_opencti_target_ref
     */
    val filter = Seq("id", "type")
     /*
     * Field rules used by OpenCTI (see stix.js)
     *
     * (1) Specify relation `from`
     *
     * If the specified type is `sighting` the field is named `sighting_of_ref`
     * and otherwise `source_ref`.  This field contains an identifier as
     * [String] and is accompanied by `x_opencti_* ` to specify the internal
     * counterpart
     *
     * (2) Specify relation `to`
     *
     * If the specified type is `sighting` the field is named `where_sighted_refs`
     * and otherwise `target_ref`. The field contains a list of identifiers
     * and is accompanied by `x_opencti_target_ref`, which contains the bare id.
     *
     * (3) Region specific input cases
     *
     * Field `x_opencti_stix_ids` contains a list of identifiers
     *
     * (4) Marking definition
     *
     * Field `name`             [String]
     * Field `definition_type`  [String]
     * Field `definition`       [Map[String,String]] where the key is equal to the
     * definition type
     *
     * (5) Object references
     *
     * Field `object_refs [List[Map[String,Any]] or List[String]
    *
     * In case of a simple list, the list items define identifiers, and
     * otherwise, it is Map of
     *
     * - reference [String]
     * - value     [String]
     * - x_opencti_internal_id [String]
     *
     * (6) Marking references
     *
     * Field `object_marking_refs` [List[Map[String,Any]] or List[String]
     *
     * In case of a simple list, the list items define identifiers, and
     * otherwise, it is Map of
     *
     * - reference [String]
     * - value     [String]
     * - x_opencti_internal_id [String]
     *
     * (6) Created By
     *
     * Field `created_by_ref` Map[String, Any] or [String]
     *
     * In case of a [String], this field defines an identifier and
     * otherwise, it is a Map of
      *
     * - reference [String]
     * - value     [String]
     * - x_opencti_internal_id [String]
     *
     * (7) Embedded relations
     *
     * Field `labels` [List[Map[String,Any]] or List[String]
     *
     * In case of a simple list, the list items define identifiers, and
     * otherwise, it is Map of
     *
     * - reference [String]
     * - value     [String]
     * - x_opencti_internal_id [String]
     *
     * (8) Kill chain phases
     *
     * Field `kill_chain_phases` [List[Map[String,Any]]
     *
     * The Map either specifies
     *
     * - kill_chain_name [String]
     * - phase_name      [String]
     *
     * or
     *
     * - reference [String]
     * - value     [String]
     * - x_opencti_internal_id [String]
     *
     * (9) External references
     *
     * Field `external_references` [List[Map[String,Any]]
     *
     * The Map either specifies
     *
     * - source_name [String]
     * - description [String]
     * - url         [String]
     * - hashes      ???
     * - external_id [String]
     *
     * or
     *
     * - reference [String]
     * - value     [String]
     * - x_opencti_internal_id [String]
     *
     * (10) Attribute filtering
     *
     * This is the final step where previous mappings are added
     * to the event as [key] = val
     *
     */

    var filteredData = data
    /*
     * The remaining part of this method distinguishes between fields
     * that describe describe relations and those that carry object
     * properties.
     */
    val batch = new JsonArray

    /*
     * Metadata information that (optionally) refer to other
     * STIX objects
     */
    val createdByRef: JsonArray = new JsonArray
    var externalRefs:JsonArray = new JsonArray

    var killChains:JsonArray = new JsonArray
    var objectLabels:JsonArray = new JsonArray

    var objectMarkings:JsonArray = new JsonArray
    var objectRefs:JsonArray = new JsonArray

    var hashes:Map[String,String] = Map.empty[String,String]

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
      val value = createCreatedBy(filteredData)
      createdByRef.add(value.getOrElse("NULL"))
      /*
       * Remove 'created_by_ref' from the provided dataset
       * to restrict further processing to object properties
       */
      filteredData = filteredData.filterKeys(k => !(k == "created_by_ref"))
    }

    /** EXTERNAL REFERENCES **/

    if (filteredData.contains("external_references")) {
      /*
       * External references are mapped onto a JsonArray of
       * serialized external objects
       */
      val references = createExternalReferences(filteredData)
      if (references.isDefined) externalRefs = references.get

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
      val killChainPhases = createKillChainPhases(filteredData)
      if (killChainPhases.isDefined) killChains = killChainPhases.get
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
      val labels = createObjectLabels(filteredData)
      if (labels.isDefined) objectLabels = labels.get
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
      val markings = createObjectMarkings(filteredData)
      if (markings.isDefined) objectMarkings = markings.get
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
      val references = createObjectReferences(filteredData)
      if (references.isDefined) objectRefs = references.get
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
       * properties to the Json object, irrespective of whether
       * the values are defined or not. This makes update
       * request a lot easier.
       */
      hashes = transformHashes(data("hashes"))
      filteredData = filteredData.filterKeys(k => !(k == "hashes"))
    }
    /*
     * Add remaining properties to the SDO or SCO; the current
     * implementation accepts properties of a basic data type
     * or a list where the components specify basic data types.
     */
    filteredData.keySet.foreach(propKey => {

      val batchObj = new JsonObject
      batchObj.addProperty("action", "create")
      batchObj.addProperty("operation", "add")

      batchObj.addProperty("entity_id", entityId)
      batchObj.addProperty("entity_type", entityType)
      /*
       * Assign references to the batch object
       */
      batchObj.add("created_by_ref", createdByRef)
      batchObj.add("external_references", externalRefs)

      batchObj.add("kill_chain_phases", killChains)
      batchObj.add("object_labels", objectLabels)

      batchObj.add("object_marking_refs", objectMarkings)
      batchObj.add("object_refs", objectRefs)
      /*
       * Assign defined hash values to the batch
       * object whether the hashes exist or not
       */
      STIX.STANDARD_HASHES.foreach(hash => {

        val value = hashes.getOrElse(hash, "NULL")
        batchObj.addProperty(hash, value)

      })
      /*
       * The property key is interpreted as attribute
       * name and harmonized with other data sources
       * like Fiware
       */
      batchObj.addProperty("attr_name", propKey)
      /*
       * Assign the property values to the batchObject
       */
      val value = filteredData(propKey)
      value match {
        case values: List[Any] =>
          try {

            val propType = getBasicType(values.head)
            batchObj.addProperty("attr_type", s"List[$propType]")

            val serialized = mapper.writeValueAsString(value)
            batchObj.addProperty("attr_value", serialized)

          } catch {
            case _:Throwable => /* Do nothing */
          }

        case _ =>
          try {

            val propType = getBasicType(value)
            batchObj.addProperty("attr_type", propType)

            val serialized = mapper.writeValueAsString(value)
            batchObj.addProperty("attr_value", serialized)

          } catch {
            case _:Throwable => /* Do nothing */
          }
      }

      batch.add(batchObj)

    })

    val fields = Array(primaryKey) ++ CTISchema.stix_object().fields
    val schema = StructType(fields)

    (schema, batch)

  }

  /** UPDATE SUPPORT  */

  def updateSighting(entityId: String, entityType: String, data: Map[String, Any]):(StructType, JsonArray) = {

    /*
     * Retrieve patch from data
     */
    val patch = getPatch(data)
    if (patch.isDefined) {

      val batch = new JsonArray

      val sightingRef: String = "NULL"
      val sightedRefs: String = "NULL"

      val patchData = patch.get
      patchData.keySet.foreach(operation => {
        val properties = patchData(operation).asInstanceOf[Map[String, List[Any]]]
        val fields = Seq(
          "attribute_count",
          "confidence",
          "count",
          "created",
          "created_by_ref",
          "description",
          "first_seen",
          "last_seen",
          "modified",
          "name")

        fields.foreach(field => {

          if (properties.contains(field)) {

            val batchObj = new JsonObject
            batchObj.addProperty("action", "create")
            batchObj.addProperty("operation", operation)

            batchObj.addProperty("entity_id", entityId)
            batchObj.addProperty("entity_type", entityType)

            batchObj.addProperty("sighting_of_ref", sightingRef)
            batchObj.addProperty("where_sighted_refs", sightedRefs)

            batchObj.addProperty("attr_name", field)

            field match {
              case "attribute_count" | "confidence" | "count" =>
                val value = properties(field).head.asInstanceOf[Int]

                batchObj.addProperty("attr_type", "INT")
                batchObj.addProperty("attr_value", value.toString)

              case _ =>
                val value = properties(field).head.asInstanceOf[String]

                batchObj.addProperty("attr_type", "STRING")
                batchObj.addProperty("attr_value", value)
            }

            batch.add(batchObj)
          }
        })
      })

      val fields = Array(primaryKey) ++ CTISchema.sighting().fields
      val schema = StructType(fields)

      (schema, batch)

    }
    else
      (emptySchema, emptyBatch)

  }

  def updateStixObject(entityId: String, entityType: String, data: Map[String, Any]):(StructType, JsonArray) = {
    /*
    * Retrieve patch data from data
    */
    val patch = getPatch(data)
    if (patch.isDefined) {

      val batch = new JsonArray
      val patchData = patch.get
      /*
       * Patch (update) data are organized with respect
       * to the associated data operation (add | remove etc)
       */
      patchData.keySet.foreach(operation => {
        var properties = patchData(operation).asInstanceOf[Map[String, List[Any]]]

        val createdByRef:JsonArray = new JsonArray
        val externalRefs:JsonArray = new JsonArray

        val killChains:JsonArray = new JsonArray
        val objectLabels:JsonArray = new JsonArray

        val objectMarkings:JsonArray = new JsonArray
        val objectRefs:JsonArray = new JsonArray

        var hashes:Map[String,String] = Map.empty[String,String]

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
              references.foreach(reference =>
                createdByRef.add(reference.asInstanceOf[String])
              )
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
              references.foreach(reference =>
                externalRefs.add(reference.asInstanceOf[String])
              )
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
              references.foreach(reference =>
                killChains.add(reference.asInstanceOf[String])
              )
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
              references.foreach(reference =>
                objectLabels.add(reference.asInstanceOf[String])
              )
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
              references.foreach(reference =>
                objectMarkings.add(reference.asInstanceOf[String])
              )
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
              references.foreach(reference =>
                objectRefs.add(reference.asInstanceOf[String])
              )
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
              hashes = values.asInstanceOf[List[Map[String,String]]]
                .map(hash => {
                  /*
                   * We expect the hash properties as [Map] with fields
                   * `algorithm` and `hash`.
                   */
                  (hash("algorithm"), hash("hash"))
                }).toMap
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

          val batchObj = new JsonObject
          batchObj.addProperty("action", "update")
          batchObj.addProperty("operation", operation)

          batchObj.addProperty("entity_id", entityId)
          batchObj.addProperty("entity_type", entityType)
          /*
           * Assign references to the batch object
           */
          batchObj.add("created_by_ref", createdByRef)
          batchObj.add("external_references", externalRefs)

          batchObj.add("kill_chain_phases", killChains)
          batchObj.add("object_labels", objectLabels)

          batchObj.add("object_marking_refs", objectMarkings)
          batchObj.add("object_refs", objectRefs)
          /*
           * Assign defined hash values to the batch
           * object whether the hashes exist or not
           */
          STIX.STANDARD_HASHES.foreach(hash => {

            val value = hashes.getOrElse(hash, "NULL")
            batchObj.addProperty(hash, value)

          })
          /*
           * The property key is interpreted as attribute
           * name and harmonized with other data sources
           * like Fiware
           */
          batchObj.addProperty("attr_name", propKey)

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
                batchObj.addProperty("attr_type", s"List[$propType]")

                val serialized = mapper.writeValueAsString(value)
                batchObj.addProperty("attr_value", serialized)

              case _ =>
                try {

                  val propType = getBasicType(value)
                  batchObj.addProperty("attr_type", s"$propType")

                  val serialized = mapper.writeValueAsString(value)
                  batchObj.addProperty("attr_value", serialized)

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

              val propType = getBasicType(values.head)
              batchObj.addProperty("attr_type", s"List[$propType]")

              val serialized = mapper.writeValueAsString(values)
              batchObj.addProperty("attr_value", serialized)

            } catch {
              case _:Throwable => /* Do nothing */
            }

          }
        })

      })

      val fields = Array(primaryKey) ++ CTISchema.stix_object().fields
      val schema = StructType(fields)

      (schema, batch)

    }
    else
      (emptySchema, emptyBatch)

  }

  /** HELPER METHODS **/

  /**
   * This method creates an object for an SDO to
   * describe the relationship to the `Identity`
   * that created the object
   */
  def createCreatedBy(data:Map[String, Any]):Option[String] = {

    val createdBy = data("created_by_ref")
    try {
      /*
       *
       * OpenCTI supports two different formats to describe
       * the created_by_ref field:
       *
       * (1) [String]: identifier
       *
       * (2) [Map[String,String]: 'reference', 'value', 'x_opencti_internal_id'
       *
       * In both cases, an identifier is provided to reference
       * the creator (identity object)
       */
      createdBy match {
        /*
         * This is the expected default description of references
         * to object markings
         */
        case value: String => Some(value)
        /*
         * The OpenCTI code base also specifies the subsequent format
         * to describe reference to the author or creator of a STIX
         * object.
         */
        case value: Map[_, _] =>
          Some(value.asInstanceOf[Map[String,String]]("value"))

        case _ =>
          val now = new Date().toString
          throw new Exception(s"[ERROR] $now - The data type of the created_by field is not supported.")
      }

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating created_by failed: ", t)
        None
    }

  }

  def createExternalReferences(data:Map[String, Any]): Option[JsonArray] = {

    try {

      val result = new JsonArray

      val references = data("external_references").asInstanceOf[List[Any]]
      references.foreach(reference => {

        val refObj = new JsonObject
        /*
         *
         * OpenCTI supports two different formats to describe
         * external references:
         *
         * (1) fields: 'source_name', 'description', 'url', 'hashes', 'external_id'
         *
         * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
         */
        reference match {
          case _: Map[_, _] =>
            val value = reference.asInstanceOf[Map[String,Any]]
            if (value.contains("value")) {
              /*
               * The `value` field contains the identifier of
               * the External Reference object
               */
              refObj
                .addProperty("external_reference",
                  value("value").asInstanceOf[String])

            }
            else {
              val fields = List(
                "source_name",
                "description",
                "url",
                "external_id")

              fields.foreach(fname => {

                val fvalue = data.getOrElse(fname, "").asInstanceOf[String]
                refObj.addProperty(fname, fvalue)

              })
              /*
               * hashes is optional, but when provided specifies a dictionary of hashes
               * for the contents of the url:
               * hashes: {
               *  "SHA-256": "..."
               * }
               */
              if (data.contains("hashes")) {

                val hashes = transformHashes(data("hashes"))
                hashes.foreach{case (k,v) => refObj.addProperty(k, v)
                }

              }
            }
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided external reference is not supported.")
        }
        /*
         * The output format is restricted to
         * ArrayType(StringType); therefore
         * reference are serialized
         */
        result.add(refObj.toString)

      })

      Some(result)

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating external references failed: ", t)
        None
    }

  }

  def createKillChainPhases(data:Map[String, Any]): Option[JsonArray] = {

    try {

      val result = new JsonArray

      val killChainPhases = data("kill_chain_phases").asInstanceOf[List[Any]]
      killChainPhases.foreach(killChainPhase => {

        val phaseObj = new JsonObject
         /*
          *
          * OpenCTI supports two different formats to describe
          * kill chain phases:
          *
          * (1) fields: 'kill_chain_name', 'phase_name'
          *
          * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
          */
        killChainPhase match {
          case _: Map[_, _] =>
            val value = killChainPhase.asInstanceOf[Map[String, Any]]
            if (value.contains("value")) {

              phaseObj
                .addProperty("kill_chain_name",
                  value("value").asInstanceOf[String])

            }
            else {

              /* kill_chain_name */
              phaseObj.addProperty("kill_chain_name",
                value.getOrElse("kill_chain_name", "").asInstanceOf[String])

              /* phase_name */
              phaseObj.addProperty("phase_name",
                data.getOrElse("phase_name", "").asInstanceOf[String])

            }

          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided kill chain phase is not supported.")
        }
        /*
         * The output format is restricted to
         * ArrayType(StringType); therefore
         * kill chain phases are serialized
         */
        result.add(phaseObj.toString)

      })

      Some(result)

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating external references failed: ", t)
        None
    }

  }

  def createObjectLabels(data:Map[String, Any]):Option[JsonArray] ={

    try {

      val result = new JsonArray

      val labels = data("labels").asInstanceOf[List[Any]]
      labels.foreach(label => {

        val labelObj = new JsonObject
        /*
         *
         * OpenCTI supports two different formats to describe
         * object labels:
         *
         * (1) [String]: the label value
         *
         * (2) fields: 'reference', 'value', 'x_opencti_internal_id'
         */
        label match {
          case _: Map[_, _] =>
            val value = label.asInstanceOf[Map[String,Any]]
            /*
             * The `value` field  contains the identifier
             * of the Object Label
             */
            labelObj.addProperty("value",
              value("value").asInstanceOf[String])
            /*
             * The `reference` field specifies the label of the
             * Object Label
             */
            labelObj.addProperty("reference",
              value("reference").asInstanceOf[String])
          /*
           * We expect this as the default approach to exchange
           * object labels of STIX objects. In this case, an extra
           * vertex is created to specify the respective label.
           */
          case value:String =>
            labelObj.addProperty("reference", value)

          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - The data type of the provided object label is not supported.")
        }
        /*
         * The output format is restricted to
         * ArrayType(StringType); therefore
         * kill chain phases are serialized
         */
        result.add(labelObj.toString)

      })

      Some(result)

    } catch {
      case t: Throwable =>
        LOGGER.error("Creating object labels failed: ", t)
        None
    }

  }
  def createObjectMarkings(data:Map[String, Any]):Option[JsonArray] = {

    try {

      val result = new JsonArray

      val markings = data("object_marking_refs").asInstanceOf[List[Any]]
      markings.foreach(marking => {
        /*
         * OpenCTI specifies object markings with different
         * formats; this implementation resolves them into
         * a list of serialized values
         */
        val value = mapper.writeValueAsString(marking)
        result.add(value)

      })

      Some(result)

    } catch {
      case t:Throwable =>
        LOGGER.error("Creating object markings failed: ", t)
        None
    }

  }

  def createObjectReferences(data:Map[String, Any]):Option[JsonArray] = {

    try {

      val result = new JsonArray

      val references = data("object_refs").asInstanceOf[List[Any]]
      references.foreach {
        /*
         * This is the default case, where a STIX object contains
         * a list of references to other STIX objects.
         */
        case value: String =>
          result.add(value)

        case reference@(_: Map[_, _]) =>
          val value = reference.asInstanceOf[Map[String, Any]]
          /*
           * The `value` of the provided Map refers to the
           * Object Marking object (see OpenCTI data model).
           */
          result.add(value("value").asInstanceOf[String])

        case _ =>
          val now = new java.util.Date().toString
          throw new Exception(s"[ERROR] $now - The data type of the provided object reference is not supported.")
      }

      Some(result)
    } catch {
      case t:Throwable =>
        LOGGER.error("Creating object references failed: ", t)
        None
    }

  }

}
