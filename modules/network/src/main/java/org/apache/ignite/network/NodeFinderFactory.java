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

package org.apache.ignite.network;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toUnmodifiableList;

import java.util.Arrays;
import org.apache.ignite.configuration.schemas.network.NodeFinderType;
import org.apache.ignite.configuration.schemas.network.NodeFinderView;

/**
 * {@link NodeFinder} factory.
 */
public class NodeFinderFactory {
    /**
     * Creates a {@link NodeFinder} based on the provided configuration.
     *
     * @param nodeFinderConfiguration Node finder configuration.
     * @return Node finder.
     */
    public static NodeFinder createNodeFinder(NodeFinderView nodeFinderConfiguration) {
        String typeString = nodeFinderConfiguration.type();

        NodeFinderType type;

        try {
            type = NodeFinderType.valueOf(typeString);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Failed to create NodeFinder " + typeString, e);
        }

        switch (type) {
            case STATIC:
                return Arrays.stream(nodeFinderConfiguration.netClusterNodes())
                        .map(NetworkAddress::from)
                        .collect(collectingAndThen(toUnmodifiableList(), StaticNodeFinder::new));

            default:
                throw new IllegalArgumentException("Unsupported NodeFinder type " + type);

        }
    }
}
