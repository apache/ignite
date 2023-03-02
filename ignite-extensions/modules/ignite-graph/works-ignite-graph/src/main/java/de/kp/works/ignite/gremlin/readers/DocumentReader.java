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
import de.kp.works.ignite.gremlin.IgniteDocument;
import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.IgniteGraphUtils;
import de.kp.works.ignite.gremlin.IgniteVertex;
import de.kp.works.ignite.gremlin.exception.IgniteGraphNotFoundException;
import de.kp.works.ignite.query.IgniteResult;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DocumentReader extends VertexReader {
	final String label;

    public DocumentReader(IgniteGraph graph,String label) {
        super(graph);
        this.label = label;
    }

    @Override
    public Vertex parse(IgniteResult result) {
        Object id = result.getId();
        if(id==null) {
        	id = IgniteGraphUtils.generateIdIfNeeded(id);
        }
        ReferenceVertexProperty<?> docId = new ReferenceVertexProperty<>(id,label,id);
        Vertex vertex = graph.findOrCreateVertex(docId);
        load(vertex, result);
        return vertex;
    }

    @Override
    public void load(Vertex vertex, IgniteResult result) {
        if (result==null || result.isEmpty()) {
            throw new IgniteGraphNotFoundException(vertex, "Vertex does not exist: " + vertex.id());
        }       
        Long createdAt = null;
        Long updatedAt = null;

        Map<String, Object> props = new HashMap<>();
        for (IgniteColumn column : result.getColumns()) {
            String colName = column.getColName();
            switch (colName.toLowerCase()) {
                case IgniteConstants.LABEL_COL_NAME:                    
                    break;
                case IgniteConstants.CREATED_AT_COL_NAME:
                    createdAt = (Long)column.getColValue();
                    break;
                case IgniteConstants.UPDATED_AT_COL_NAME:
                    updatedAt = (Long)column.getColValue();
                    break;
                case "_key":                    
                    break;
                default:
                	props.put(colName, column.getColValue());
                    break;
            }
        }

        IgniteDocument newVertex = new IgniteDocument(graph, vertex.id(), vertex.label(), createdAt, updatedAt, props);
        ((IgniteVertex) vertex).copyFrom(newVertex);
    }
}
