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
import org.slf4j.{Logger, LoggerFactory}

import java.util.{Date, UUID}
import scala.collection.mutable

trait BaseUtil {

  val LOGGER: Logger = LoggerFactory.getLogger(classOf[BaseUtil])

  protected val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

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
          case _:Throwable =>
            val now = new Date().toString
            throw new Exception(s"[ERROR] $now - Unknown data type for hashes detected.")
        }
    }
    result.toMap

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
  protected def getPatch(data: Map[String, Any]): Option[Map[String,Any]] = {

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
