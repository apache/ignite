package de.kp.works.ignite.writer
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


import de.kp.works.ignite.{IgniteAdmin, IgniteConnect, IgniteConstants, IgniteTable}
import de.kp.works.ignite.mutate._

abstract class GraphWriter(connect:IgniteConnect) {

  private def admin = new IgniteAdmin(connect)

  protected def writeEdges(edges:Seq[IgniteMutation]):Unit = {

    val name = s"${connect.graphNS}_${IgniteConstants.EDGES}"
    val table = new IgniteTable(name, admin)

    edges.foreach(edge => {
      edge.mutationType match {
        case IgniteMutationType.DELETE =>
          table.delete(edge.asInstanceOf[IgniteDelete])
        case IgniteMutationType.INCREMENT =>
          table.increment(edge.asInstanceOf[IgniteIncrement])
        case IgniteMutationType.PUT =>
          table.put(edge.asInstanceOf[IgnitePut])
      }
    })

  }

  protected def writeVertices(vertices:Seq[IgniteMutation]):Unit = {

    val name = s"${connect.graphNS}_${IgniteConstants.VERTICES}"
    val table = new IgniteTable(name, admin)

    vertices.foreach(vertex => {
      vertex.mutationType match {
        case IgniteMutationType.DELETE =>
          table.delete(vertex.asInstanceOf[IgniteDelete])
        case IgniteMutationType.INCREMENT =>
          table.increment(vertex.asInstanceOf[IgniteIncrement])
        case IgniteMutationType.PUT =>
          table.put(vertex.asInstanceOf[IgnitePut])
      }
    })

  }

}
