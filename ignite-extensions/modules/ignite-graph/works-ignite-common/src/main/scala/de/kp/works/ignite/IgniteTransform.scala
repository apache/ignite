package de.kp.works.ignite

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

import de.kp.works.ignite.graph.{IgniteEdgeEntry, IgniteVertexEntry}
import de.kp.works.ignite.query.IgniteResult

import scala.collection.JavaConversions._

object IgniteTransform {

  def transformEdgeEntries(entries: java.util.List[IgniteEdgeEntry]): java.util.List[IgniteResult] = {
    entries
      .groupBy(entry => entry.id)
      .map { case (_, values) =>

        val igniteResult = new IgniteResult()
        /*
         * Extract common fields
         */
        val head = values.head
        val id: String = head.id

        val idType: String = head.idType
        val label: String = head.label

        val toId: String = head.toId
        val toIdType: String = head.toIdType

        val fromId: String = head.fromId
        val fromIdType: String = head.fromIdType

        val createdAt: Long = head.createdAt.asInstanceOf[Long]
        val updatedAt: Long = head.updatedAt.asInstanceOf[Long]
        /*
         * Add common fields
         */
        igniteResult
          .addColumn(IgniteConstants.ID_COL_NAME, idType, id)

        igniteResult
          .addColumn(IgniteConstants.LABEL_COL_NAME, ValueType.STRING.name(), label)

        igniteResult
          .addColumn(IgniteConstants.TO_COL_NAME, toIdType, toId)

        igniteResult
          .addColumn(IgniteConstants.FROM_COL_NAME, fromIdType, fromId)

        igniteResult
          .addColumn(IgniteConstants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt)

        igniteResult
          .addColumn(IgniteConstants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt)

        igniteResult

      }.toList

  }

  def transformVertexEntries(entries: java.util.List[IgniteVertexEntry]): java.util.List[IgniteResult] = {
    entries
      .groupBy(entry => entry.id)
      .map { case (_, values) =>

        val igniteResult = new IgniteResult()
        /*
         * Extract common fields
         */
        val head = values.head
        val id: String = head.id

        val idType: String = head.idType
        val label: String = head.label

        val createdAt: Long = head.createdAt.asInstanceOf[Long]
        val updatedAt: Long = head.updatedAt.asInstanceOf[Long]
        /*
         * Add common fields
         */
        igniteResult
          .addColumn(IgniteConstants.ID_COL_NAME, idType, id)

        igniteResult
          .addColumn(IgniteConstants.LABEL_COL_NAME, ValueType.STRING.name(), label)

        igniteResult
          .addColumn(IgniteConstants.CREATED_AT_COL_NAME, ValueType.LONG.name(), createdAt)

        igniteResult
          .addColumn(IgniteConstants.UPDATED_AT_COL_NAME, ValueType.LONG.name(), updatedAt)
        /*
         * Extract & add properties
         */
        values.foreach(value => {

          val propKey: String = value.propKey
          val propType: String = value.propType
          val propValue: Object = value.propValue

          igniteResult
            .addColumn(propKey, propType, propValue)

        })

        igniteResult

      }.toList

  }

}
