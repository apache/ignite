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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * DHT transaction mapping.
 */
public class GridDhtTxMapping {
    /** Transaction nodes mapping (primary node -> related backup nodes). */
    private final Map<UUID, Collection<UUID>> txNodes = new GridLeanMap<>();

    /**
     * Adds information about next mapping.
     *
     * @param nodes Nodes.
     */
    public void addMapping(List<ClusterNode> nodes) {
        assert !F.isEmpty(nodes) : nodes;

        ClusterNode primary = nodes.get(0);

        int size = nodes.size();

        if (size > 1) {
            Collection<UUID> backups = txNodes.get(primary.id());

            if (backups == null) {
                backups = U.newHashSet(size - 1);

                txNodes.put(primary.id(), backups);
            }

            for (int i = 1; i < size; i++)
                backups.add(nodes.get(i).id());
        }
        else
            txNodes.put(primary.id(), new GridLeanSet<UUID>());
    }

    /**
     * @return Primary to backup mapping.
     */
    public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }
}
