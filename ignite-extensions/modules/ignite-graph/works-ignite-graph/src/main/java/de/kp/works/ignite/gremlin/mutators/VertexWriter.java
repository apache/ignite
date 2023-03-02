package de.kp.works.ignite.gremlin.mutators;
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

import de.kp.works.ignite.IgniteConstants;
import de.kp.works.ignite.ValueUtils;
import de.kp.works.ignite.graph.ElementType;
import de.kp.works.ignite.mutate.IgnitePut;
import de.kp.works.ignite.gremlin.*;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

public final class VertexWriter implements Creator {

    private final Vertex vertex;

    public VertexWriter(IgniteGraph graph, Vertex vertex) {
        this.vertex = vertex;
    }

    @Override
    public Vertex getElement() {
        return vertex;
    }

    @Override
    public Iterator<IgnitePut> constructInsertions() {

        final String label = vertex.label() != null ? vertex.label() : Vertex.DEFAULT_LABEL;

        Object id = vertex.id();

        IgnitePut put = new IgnitePut(id, ElementType.VERTEX);
        put.addColumn(IgniteConstants.ID_COL_NAME, ValueUtils.getValueType(id).name(), id);

        put.addColumn(IgniteConstants.LABEL_COL_NAME, IgniteConstants.STRING_COL_TYPE, label);

        Long createdAt = ((IgniteVertex) vertex).createdAt();
        put.addColumn(IgniteConstants.CREATED_AT_COL_NAME, IgniteConstants.LONG_COL_TYPE, createdAt);

        Long updatedAt = ((IgniteVertex) vertex).updatedAt();
        put.addColumn(IgniteConstants.UPDATED_AT_COL_NAME, IgniteConstants.LONG_COL_TYPE, updatedAt);

        ((IgniteVertex) vertex).getProperties().forEach((key, value) -> {
            String colType = ValueUtils.getValueType(value).name();
            Object colValue = value;

            put.addColumn(key, colType, colValue);
        });

        return IteratorUtils.of(put);
    }

    @Override
    public RuntimeException alreadyExists() {
        return Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
    }
}
