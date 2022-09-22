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
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.IgniteVertex;
import de.kp.works.ignite.gremlin.exception.IgniteGraphNotFoundException;
import de.kp.works.ignite.query.IgniteResult;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.Map;

public class VertexReader extends LoadingElementReader<Vertex> {

    public VertexReader(IgniteGraph graph) {
        super(graph);
    }

    @Override
    public Vertex parse(IgniteResult result) {
        Object id = result.getId();
        Vertex vertex = graph.findOrCreateVertex(id);
        load(vertex, result);
        return vertex;
    }

    @Override
    public void load(Vertex vertex, IgniteResult result) {
        if (result.isEmpty()) {
            throw new IgniteGraphNotFoundException(vertex, "Vertex does not exist: " + vertex.id());
        }
        String label   = null;
        Long createdAt = null;
        Long updatedAt = null;

        Map<String, Object> props = new HashMap<>();
        for (IgniteColumn column : result.getColumns()) {
            String colName = column.getColName();
            switch (colName) {
                case IgniteConstants.LABEL_COL_NAME:
                    label = column.getColValue().toString();
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

        IgniteVertex newVertex = new IgniteVertex(graph, vertex.id(), label, createdAt, updatedAt, props);
        ((IgniteVertex) vertex).copyFrom(newVertex);
    }
}
