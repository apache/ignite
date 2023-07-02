package de.kp.works.ignite.streamer.beat.graph

/**
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
import de.kp.works.ignite.mutate.IgniteMutation
import de.kp.works.ignite.sse.SseEvent

import scala.collection.mutable

object BeatTransformer {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  /**
   * Events format :: SseEvent(id, event, data)
   */
  def transform(sseEvents: Seq[SseEvent]): (Seq[IgniteMutation], Seq[IgniteMutation]) = {

    val vertices = mutable.ArrayBuffer.empty[IgniteMutation]
    val edges = mutable.ArrayBuffer.empty[IgniteMutation]

    // TODO

    (vertices, edges)

  }

}
