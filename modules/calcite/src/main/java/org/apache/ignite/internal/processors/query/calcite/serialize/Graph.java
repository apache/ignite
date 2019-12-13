/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.GridIntList;

/**
 *
 */
public class Graph<T extends GraphNode> implements Serializable {
    private final List<T> nodes = new ArrayList<>();
    private final List<GridIntList> edges = new ArrayList<>();

    public List<Ord<T>> nodes() {
        return Ord.zip(nodes);
    }

    public List<GridIntList> edges() {
        return Commons.transform(edges, GridIntList::copy);
    }

    public int addNode(int parentId, T node) {
        int id = addNode(node);

        addEdge(parentId, id);

        return id;
    }

    public int addNode(T node) {
        assert nodes.size() == edges.size();

        int id = nodes.size();

        nodes.add(node);
        edges.add(new GridIntList());

        return id;
    }

    public void addEdge(int parentId, int childId) {
        assert parentId == -1 || (parentId >= 0 && parentId < edges.size());
        assert nodes.size() == edges.size();

        if (parentId != -1)
            edges.get(parentId).add(childId);
    }

    public List<Ord<T>> children(int parentId) {
        GridIntList children = edges.get(parentId);

        ArrayList<Ord<T>> ords = new ArrayList<>(children.size());

        for (int i = 0; i < children.size(); i++)
            ords.add(Ord.of(children.get(i), nodes.get(children.get(i))));

        return ords;
    }
}
