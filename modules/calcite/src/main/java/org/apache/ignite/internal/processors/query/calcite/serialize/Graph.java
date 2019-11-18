/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.util.GridIntList;

/**
 *
 */
public class Graph {
    private final List<GraphNode> nodes = new ArrayList<>();
    private final List<GridIntList> edges = new ArrayList<>();

    int addNode(GraphNode node) {
        assert nodes.size() == edges.size();

        int id = nodes.size();

        nodes.add(node);
        edges.add(new GridIntList());

        return id;
    }

    void addEdge(int parentId, int childId) {
        edges.get(parentId).add(childId);
    }

    int addChild(int parentId, GraphNode node) {
        int id = addNode(node);

        edges.get(parentId).add(id);

        return id;
    }

    List<GraphNode> children(int parentId) {
        GridIntList childrenIds = edges.get(parentId);
        ArrayList<GraphNode> children = new ArrayList<>(childrenIds.size());

        for (int i = 0; i < childrenIds.size(); i++) {
            children.add(nodes.get(i));
        }

        return children;
    }
}
