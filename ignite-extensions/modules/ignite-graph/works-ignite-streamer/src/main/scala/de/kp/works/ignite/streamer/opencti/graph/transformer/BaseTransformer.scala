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

import de.kp.works.ignite.IgniteConstants
import de.kp.works.ignite.graph.ElementType
import de.kp.works.ignite.mutate._
import org.slf4j.{Logger, LoggerFactory}

import java.util.{Date, UUID}
import scala.collection.mutable

trait BaseTransformer {

  val LOGGER: Logger = LoggerFactory.getLogger(classOf[BaseTransformer])

  val EXTERNAL_REFERENCE: String = "external-reference"
  val KILL_CHAIN_PHASE: String = "kill-chain-phase"
  val OBJECT_LABEL: String = "object-label"
  /**
   * INTERNAL EDGE LABELS
   */
  val HAS_CREATED_BY: String = "has-created-by"
  val HAS_EXTERNAL_REFERENCE: String = "has-external-reference"
  val HAS_KILL_CHAIN_PHASE: String = "has-kill-chain-phase"
  val HAS_OBJECT_LABEL: String = "has-object-label"
  val HAS_OBJECT_MARKING: String = "has-object-marking"
  val HAS_OBJECT_REFERENCE: String = "has-object-reference"

  protected def deleteEdge(fromId:String, toId:String):Option[IgniteDelete] = {
    /*
     * This [edge] must be deleted by referencing the
     * `from` and `to` identifier
     */
    val edge = new IgniteDelete(null, ElementType.EDGE)
    edge.addColumn(
      IgniteConstants.FROM_COL_NAME, "STRING", fromId)

    edge.addColumn(
      IgniteConstants.TO_COL_NAME, "STRING", toId)

    Some(edge)

  }

  /**
   * A helper method to create an [IgnitePut] and assign
   * identifier and type.
   */
  protected def initializeEdge(entityId: String, entityType: String, action:String): IgnitePut = {

    val edge = new IgnitePut(entityId, ElementType.EDGE)
    /*
     * The data type is known [String] and synchronized with ValueType
     * note, the provided entity type is used to specify the edge label
     */
    edge.addColumn(
      IgniteConstants.ID_COL_NAME, "STRING", entityId)

    edge.addColumn(
      IgniteConstants.LABEL_COL_NAME, "STRING", entityType.toLowerCase())
    /*
     * Assign time management fields: These fields are internal
     * fields and are not synchronized with potentially existing
     * ones that have the same meaning.
     */
    val timestamp = System.currentTimeMillis()
    if (action == "create") {
      edge.addColumn(
        IgniteConstants.CREATED_AT_COL_NAME, "LONG", timestamp.toString)
    }

    edge.addColumn(
      IgniteConstants.UPDATED_AT_COL_NAME, "LONG", timestamp.toString)

    edge
  }

  /**
   * A helper method to create an [IgnitePut] and assign
   * identifier and type.
   */
  protected def initializeVertex(entityId: String, entityType: String, action:String): IgnitePut = {

    val vertex = new IgnitePut(entityId, ElementType.VERTEX)
    /*
     * The data type is known [String] and synchronized with ValueType;
     * note, the provided entity type is used to specify the vertex label
     */
    vertex.addColumn(
      IgniteConstants.ID_COL_NAME, "STRING", entityId)

    vertex.addColumn(
      IgniteConstants.LABEL_COL_NAME, "STRING", entityType.toLowerCase())
    /*
     * Assign time management fields: These fields are internal
     * fields and are not synchronized with potentially existing
     * ones that have the same meaning.
     */
    val timestamp = System.currentTimeMillis()
    if (action == "create") {
      vertex.addColumn(
        IgniteConstants.CREATED_AT_COL_NAME, "LONG", timestamp.toString)
    }

    vertex.addColumn(
      IgniteConstants.UPDATED_AT_COL_NAME, "LONG", timestamp.toString)

    vertex
  }

  protected def transformHashes(hashes: Any): Map[String, String] = {

    val result = mutable.HashMap.empty[String, String]
    /*
     * Flatten hashes
     */
    hashes match {
      case _: List[Any] =>
        hashes.asInstanceOf[List[Map[String, String]]].foreach(hash => {
          val k = hash("algorithm")
          val v = hash("hash")

          result += k -> v
        })
      case _ =>
        try {
          hashes.asInstanceOf[Map[String,String]].foreach(entry => {
            result += entry._1 -> entry._2.asInstanceOf[String]
          })

        } catch {
          case t:Throwable =>
            val now = new Date().toString
            throw new Exception(s"[ERROR] $now - Unknown data type for hashes detected.")
        }
    }
    result.toMap

  }

  protected def putValues(propKey: String, basicType: String, propVal: List[Any], put: IgnitePut): Unit = {

    val propType = s"List[$basicType]"
    basicType match {
      /*
       * Basic data types
       */
      case "DECIMAL" =>
        val values = propVal.map(_.asInstanceOf[BigDecimal])
        put.addColumn(propKey, propType, values.mkString(","))
      case "BOOLEAN" =>
        val values = propVal.map(_.asInstanceOf[Boolean])
        put.addColumn(propKey, propType, values.mkString(","))
      case "BYTE" =>
        val values = propVal.map(_.asInstanceOf[Byte])
        put.addColumn(propKey, propType, values.mkString(","))
      case "DOUBLE" =>
        val values = propVal.map(_.asInstanceOf[Double])
        put.addColumn(propKey, propType, values.mkString(","))
      case "FLOAT" =>
        val values = propVal.map(_.asInstanceOf[Float])
        put.addColumn(propKey, propType, values.mkString(","))
      case "INT" =>
        val values = propVal.map(_.asInstanceOf[Int])
        put.addColumn(propKey, propType, values.mkString(","))
      case "LONG" =>
        val values = propVal.map(_.asInstanceOf[Long])
        put.addColumn(propKey, propType, values.mkString(","))
      case "SHORT" =>
        val values = propVal.map(_.asInstanceOf[Short])
        put.addColumn(propKey, propType, values.mkString(","))
      case "STRING" =>
        val values = propVal.map(_.asInstanceOf[String])
        put.addColumn(propKey, propType, values.mkString(","))
      /*
       * Datetime support
       */
      case "Date" =>
        propVal.head match {
          case _: java.sql.Date =>
            val values = propVal.map(_.asInstanceOf[java.sql.Date])
            put.addColumn(propKey, propType, values.mkString(","))
          case _: java.util.Date =>
            val values = propVal.map(_.asInstanceOf[java.util.Date])
            put.addColumn(propKey, propType, values.mkString(","))
          case _: java.time.LocalDate =>
            val values = propVal.map(_.asInstanceOf[java.time.LocalDate])
            put.addColumn(propKey, propType, values.mkString(","))
          case _: java.time.LocalDateTime =>
            val values = propVal.map(_.asInstanceOf[java.time.LocalDateTime])
            put.addColumn(propKey, propType, values.mkString(","))
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - Date data type not supported.")
        }
      case "TIMESTAMP" =>
        propVal.head match {
          case _: java.sql.Timestamp =>
            val values = propVal.map(_.asInstanceOf[java.sql.Timestamp])
            put.addColumn(propKey, propType, values.mkString(","))
          case _: java.time.LocalTime =>
            val values = propVal.map(_.asInstanceOf[java.time.LocalTime])
            put.addColumn(propKey, propType, values.mkString(","))
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - Timestamp data type not supported.")
        }
      /*
       * Handpicked data types
       */
      case "UUID" =>
        val values = propVal.map(_.asInstanceOf[java.util.UUID])
        put.addColumn(propKey, propType, values.mkString(","))
      case _ =>
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Basic data type not supported.")
    }

  }

  protected def putValue(propKey: String, propType: String, propVal: Any, put: IgnitePut): Unit = {
     propType match {
      /*
       * Basic data types
       */
      case "DECIMAL" =>
        val value = propVal.asInstanceOf[BigDecimal].toString
        put.addColumn(propKey, propType, value)
      case "BOOLEAN" =>
        val value = propVal.asInstanceOf[Boolean]
        put.addColumn(propKey, propType, value.toString)
      case "BYTE" =>
        val value = propVal.asInstanceOf[Byte]
        put.addColumn(propKey, propType, value.toString)
      case "DOUBLE" =>
        val value = propVal.asInstanceOf[Double]
        put.addColumn(propKey, propType, value.toString)
      case "FLOAT" =>
        val value = propVal.asInstanceOf[Float]
        put.addColumn(propKey, propType, value.toString)
      case "INT" =>
        val value = propVal.asInstanceOf[Int]
        put.addColumn(propKey, propType, value.toString)
      case "LONG" =>
        val value = propVal.asInstanceOf[Long]
        put.addColumn(propKey, propType, value.toString)
      case "SHORT" =>
        val value = propVal.asInstanceOf[Short]
        put.addColumn(propKey, propType, value.toString)
      case "STRING" =>
        val value = propVal.asInstanceOf[String]
        put.addColumn(propKey, propType, value)
      /*
       * Datetime support
       */
      case "DATE" =>
        propVal match {
          case _: java.sql.Date =>
            val value = propVal.asInstanceOf[java.sql.Date]
            put.addColumn(propKey, propType, value.toString)
          case _: java.util.Date =>
            val value = propVal.asInstanceOf[java.util.Date]
            put.addColumn(propKey, propType, value.toString)
          case _: java.time.LocalDate =>
            val value = propVal.asInstanceOf[java.time.LocalDate]
            put.addColumn(propKey, propType, value.toString)
          case _: java.time.LocalDateTime =>
            val value = propVal.asInstanceOf[java.time.LocalDateTime]
            put.addColumn(propKey, propType, value.toString)
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - Date data type not supported.")
        }
      case "TIMESTAMP" =>
        propVal match {
          case _: java.sql.Timestamp =>
            val value = propVal.asInstanceOf[java.sql.Timestamp]
            put.addColumn(propKey, propType, value.toString)
          case _: java.time.LocalTime =>
            val value = propVal.asInstanceOf[java.time.LocalTime]
            put.addColumn(propKey, propType, value.toString)
          case _ =>
            val now = new java.util.Date().toString
            throw new Exception(s"[ERROR] $now - Timestamp data type not supported.")
        }
      /*
       * Handpicked data types
       */
      case "UUID" =>
        val value = propVal.asInstanceOf[java.util.UUID]
        put.addColumn(propKey, propType, value.toString)
      case _ =>
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Basic data type not supported.")
    }
  }

  protected def getBasicType(attrVal: Any): String = {
    attrVal match {
      /*
       * Basic data types: these data type descriptions
       * are harmonized with [ValueType]
       */
      case _: BigDecimal => "DECIMAL"
      case _: Boolean => "BOOLEAN"
      case _: Byte => "BYTE"
      case _: Double => "DOUBLE"
      case _: Float => "FLOAT"
      case _: Int => "INT"
      case _: Long => "LONG"
      case _: Short => "SHORT"
      case _: String => "STRING"
      /*
       * Datetime support
       */
      case _: java.sql.Date => "DATE"
      case _: java.sql.Timestamp => "TIMESTAMP"
      case _: java.util.Date => "DATE"
      case _: java.time.LocalDate => "DATE"
      case _: java.time.LocalDateTime => "DATE"
      case _: java.time.LocalTime => "TIMESTAMP"
      /*
       * Handpicked data types
       */
      case _: UUID => "UUID"
      case _ =>
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Basic data type not supported.")
    }

  }

  /*
   * SAMPLE UPDATE EVENT
   *
   *   data: {
   *     x_opencti_patch: {
   *       replace: { threat_actor_types: { current: ['competitor', 'crime-syndicate'], previous: ['competitor'] } },
   *     },
   *     id: 'threat-actor--b3486bf4-2cf8-527c-ab40-3fd2ff54de77',
   *     x_opencti_id: 'f499ceab-b3bf-4f39-827d-aea43beed391',
   *     type: 'threat-actor',
   *   }
   *
   */
  def getPatch(data: Map[String, Any]): Option[Map[String,Any]] = {

    val patch = {
      if (data.contains("x_opencti_patch")) {
        data("x_opencti_patch").asInstanceOf[Map[String, Any]]
      }
      else
        Map.empty[String, Any]
    }

    if (patch.isEmpty) return None
    /*
     * The patch contains a set of operations,
     * where an operation can be `add`, `remove`
     * or `replace`
     */
    val patchData = mutable.HashMap.empty[String, Any]

    val operations = patch.keySet
    /*
     * It is expected that OpenCTI specifies a maximum
     * of 3 update operations
     */
    operations.foreach {
      case "add" =>
        /*
         * The patch specifies a set of object properties
         * where values must be added
         */
        val filteredPatch = patch.get("add").asInstanceOf[Map[String,Any]]
        /*
         * Unpack reference properties and the associated
         * values that must be added; the result is a [Map]
         * with propKey -> propValues
         */
        val properties = filteredPatch.keySet.map(propKey => {

          val propVal = filteredPatch(propKey).asInstanceOf[List[Any]]
          val values =
            if (propVal.head.isInstanceOf[Map[_,_]]) {
              propVal.map(value => {
                value.asInstanceOf[Map[String,Any]]("value")
              })
            }
            else
              propVal

          (propKey, values)

        }).toMap

        patchData += "add" -> properties

      case "remove" =>
        /*
         * The patch specifies a set of object properties
         * where values must be removed
         */
        val filteredPatch = patch.get("remove").asInstanceOf[Map[String,Any]]
        /*
         * Unpack reference properties and the associated
         * values that must be removed; the result is a [Map]
         * with propKey -> propValues
         */
        val properties = filteredPatch.keySet.map(propKey => {

          val propVal = filteredPatch(propKey).asInstanceOf[List[Any]]
          val values =
            if (propVal.head.isInstanceOf[Map[_,_]]) {
              propVal.map(value => {
                value.asInstanceOf[Map[String,Any]]("value")
              })
            }
            else
              propVal

          (propKey, values)

        }).toMap

        patchData += "remove" -> properties

      case "replace" =>
        /*
        * The patch specifies a set of object properties
        * where values must be replaced
        */
        val filteredPatch = patch.get("replace").asInstanceOf[Map[String,Any]]
        /*
         * Unpack reference properties and the current
         * values that must be set
         */
        val properties = filteredPatch.keySet.map(propKey => {

          val propVal = filteredPatch(propKey).asInstanceOf[Map[String,Any]]("current")
          /*
           * The referenced value(s) are represented as
           * a [List] to be compliant with the other patch
           * operations
           */
          val values =
            if (propVal.isInstanceOf[List[Any]]) {
              propVal
            }
            else
              List(propVal)

          (propKey, values)

        }).toMap

        patchData += "replace" -> properties

     case _ =>
        val now = new java.util.Date().toString
        throw new Exception(s"[ERROR] $now - Unknown patch operation detected.")
    }

    Some(patchData.toMap)

  }
}