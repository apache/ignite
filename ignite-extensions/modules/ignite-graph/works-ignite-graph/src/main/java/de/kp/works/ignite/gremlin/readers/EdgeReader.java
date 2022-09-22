package de.kp.works.ignite.gremlin.readers;
/*
 * Copyright (c) 20129 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
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

import de.kp.works.ignite.IgniteColumn;
import de.kp.works.ignite.IgniteConstants;
import de.kp.works.ignite.query.IgniteResult;
import de.kp.works.ignite.gremlin.IgniteEdge;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.exception.IgniteGraphNotFoundException;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.HashMap;
import java.util.Map;

public class EdgeReader extends LoadingElementReader<Edge> {

    public EdgeReader(IgniteGraph graph) {
        super(graph);
    }

    @Override
    public Edge parse(IgniteResult result) {
        Object id = result.getId();
        Edge edge = graph.findOrCreateEdge(id);
        load(edge, result);
        return edge;
    }

    @Override
    public void load(Edge edge, IgniteResult result) {
        if (result.isEmpty()) {
            throw new IgniteGraphNotFoundException(edge, "Edge does not exist: " + edge.id());
        }
        Object inVertexId = null;
        Object outVertexId = null;

        String label = null;

        Long createdAt = null;
        Long updatedAt = null;

        Map<String, Object> props = new HashMap<>();
        for (IgniteColumn column : result.getColumns()) {
            String colName = column.getColName();
            switch (colName) {
                case IgniteConstants.LABEL_COL_NAME:
                    label = column.getColValue().toString();
                    break;
                case IgniteConstants.FROM_COL_NAME:
                    outVertexId = column.getColValue();
                    break;
                case IgniteConstants.TO_COL_NAME:
                    inVertexId = column.getColValue();
                    break;
                case IgniteConstants.CREATED_AT_COL_NAME:
                    createdAt = (Long)column.getColValue();
                    break;
                case IgniteConstants.UPDATED_AT_COL_NAME:
                    updatedAt = (Long)column.getColValue();
                    break;
                default:
                    props.put(colName, column.getColValue());
                    break;
            }

        }

        if (inVertexId != null && outVertexId != null && label != null) {
            IgniteEdge newEdge = new IgniteEdge(graph, edge.id(), label, createdAt, updatedAt, props,
                    graph.findOrCreateVertex(inVertexId),
                    graph.findOrCreateVertex(outVertexId));
            ((IgniteEdge) edge).copyFrom(newEdge);
        } else {
            throw new IllegalStateException("Unable to parse edge from cells");
        }
    }
}
