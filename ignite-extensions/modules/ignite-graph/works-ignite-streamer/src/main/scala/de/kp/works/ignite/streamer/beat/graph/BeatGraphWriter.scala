package de.kp.works.ignite.streamer.beat.graph

/**
 * Copyright (c) 2019 - 2022 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.IgniteConnect
import de.kp.works.ignite.sse.SseEvent
import de.kp.works.ignite.writer.GraphWriter

class BeatGraphWriter(connect:IgniteConnect) extends GraphWriter(connect) {

  def write(events:Seq[SseEvent]):Unit = {
    /*
     * Leverage the BeatTransformer to extract
     * vertices and edges from the SSE events
     */
    val transformer = BeatTransformer
    val (vertices, edges) = transformer.transform(events)
    /*
     * Finally write vertices and edges to the
     * respective output caches
     */
    writeVertices(vertices)
    writeEdges(edges)

  }

}
