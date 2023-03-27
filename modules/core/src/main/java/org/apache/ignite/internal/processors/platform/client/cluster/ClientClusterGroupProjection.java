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

package org.apache.ignite.internal.processors.platform.client.cluster;

import org.apache.ignite.binary.BinaryRawReader;
import org.apache.ignite.cluster.ClusterGroup;

/**
 * Client cluster group projection representation.
 * Decodes a remote projection request from a client node.
 */
public class ClientClusterGroupProjection {
    /** */
    private static final short ATTRIBUTE = 1;

    /** */
    private static final short SERVER_NODES = 2;

    /**
     * Projection items.
     */
    private final ProjectionItem[] prjItems;

    /**
     * Constructor.
     *
     * @param prjItems Projection items.
     */
    private ClientClusterGroupProjection(ProjectionItem[] prjItems) {
        this.prjItems = prjItems;
    }

    /**
     * Reads projection from a stream.
     *
     * @param reader Reader.
     * @return Projection.
     */
    public static ClientClusterGroupProjection read(BinaryRawReader reader) {
        int cnt = reader.readInt();
        ProjectionItem[] items = cnt == 0 ? null : new ProjectionItem[cnt];
        for (int i = 0; i < cnt; i++) {
            short code = reader.readShort();
            switch (code) {
                case ATTRIBUTE: {
                    items[i] = new ForAttributeProjectionItem(reader);
                    break;
                }
                case SERVER_NODES: {
                    items[i] = new ForServerNodesProjectionItem(reader);
                    break;
                }
                default:
                    throw new UnsupportedOperationException("Unknown code: " + code);
            }
        }
        return new ClientClusterGroupProjection(items);
    }

    /**
     * Applies projection.
     *
     * @param clusterGrp Source cluster group.
     * @return New cluster group instance with the projection.
     */
    public ClusterGroup apply(ClusterGroup clusterGrp) {
        if (prjItems != null) {
            for (ProjectionItem item : prjItems)
                clusterGrp = item.apply(clusterGrp);
        }
        return clusterGrp;
    }

    /**
     * Projection item.
     */
    private interface ProjectionItem {
        /**
         * Applies projection to the cluster group.
         *
         * @param clusterGrp Cluster group.
         * @return Cluster group with current projection.
         */
        ClusterGroup apply(ClusterGroup clusterGrp);
    }

    /**
     * Attribute projection item.
     */
    private static final class ForAttributeProjectionItem implements ProjectionItem {
        /**
         * Attribute name.
         */
        private final String name;

        /**
         * Attribute value.
         */
        private final Object val;

        /**
         * Ctor.
         *
         * @param reader Reader.
         */
        public ForAttributeProjectionItem(BinaryRawReader reader) {
            name = reader.readString();
            val = reader.readObject();
        }

        /**
         * {@inheritDoc}
         */
        @Override public ClusterGroup apply(ClusterGroup clusterGrp) {
            return clusterGrp.forAttribute(name, val);
        }
    }

    /**
     * Represents server nodes only projection item.
     */
    private static final class ForServerNodesProjectionItem implements ProjectionItem {
        /**
         * Is for server nodes only.
         */
        private final Boolean isForSrvNodes;

        /**
         * Ctor.
         *
         * @param reader Reader.
         */
        public ForServerNodesProjectionItem(BinaryRawReader reader) {
            isForSrvNodes = reader.readBoolean();
        }

        /**
         * {@inheritDoc}
         */
        @Override public ClusterGroup apply(ClusterGroup clusterGrp) {
            return isForSrvNodes ? clusterGrp.forServers() : clusterGrp.forClients();
        }
    }
}
