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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.cache.distributed.GridDistributedTxMapping;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * DHT transaction mapping.
 */
public class GridDhtTxMapping {
    /** Transaction nodes mapping (primary node -> related backup nodes). */
    private final Map<UUID, Collection<UUID>> txNodes = new GridLeanMap<>();

    /** */
    private final List<TxMapping> mappings = new ArrayList<>();

    /** */
    private TxMapping last;

    /**
     * Adds information about next mapping.
     *
     * @param nodes Nodes.
     */
    @SuppressWarnings("ConstantConditions")
    public void addMapping(List<ClusterNode> nodes) {
        ClusterNode primary = F.first(nodes);

        if (last == null || !last.primary.equals(primary.id())) {
            last = new TxMapping(primary);

            mappings.add(last);
        }

        Collection<UUID> storedBackups = txNodes.get(last.primary);

        if (storedBackups == null)
            txNodes.put(last.primary, storedBackups = new HashSet<>());

        // Add backups.
        for (int i = 1; i < nodes.size(); i++) {
            ClusterNode backup = nodes.get(i);

            last.add(backup);

            storedBackups.add(backup.id());
        }
    }

    /**
     * @return Primary to backup mapping.
     */
    public Map<UUID, Collection<UUID>> transactionNodes() {
        return txNodes;
    }

    /**
     * For each mapping sets flags indicating if mapping is last for node.
     *
     * @param mappings Mappings.
     */
    public void initLast(Collection<GridDistributedTxMapping> mappings) {
        assert this.mappings.size() == mappings.size();

        int idx = 0;

        for (GridDistributedTxMapping map : mappings) {
            TxMapping mapping = this.mappings.get(idx);

            map.lastBackups(lastBackups(mapping, idx));

            boolean last = true;

            for (int i = idx + 1; i < this.mappings.size(); i++) {
                TxMapping nextMap = this.mappings.get(i);

                if (nextMap.primary.equals(mapping.primary)) {
                    last = false;

                    break;
                }
            }

            map.last(last);

            idx++;
        }
    }

    /**
     * @param mapping Mapping.
     * @param idx Mapping index.
     * @return IDs of backup nodes receiving last prepare request during this mapping.
     */
    @Nullable private Collection<UUID> lastBackups(TxMapping mapping, int idx) {
        Collection<UUID> res = null;

        for (UUID backup : mapping.backups) {
            boolean foundNext = false;

            for (int i = idx + 1; i < mappings.size(); i++) {
                TxMapping nextMap = mappings.get(i);

                if (nextMap.primary.equals(mapping.primary) && nextMap.backups.contains(backup)) {
                    foundNext = true;

                    break;
                }
            }

            if (!foundNext) {
                if (res == null)
                    res = new ArrayList<>(mapping.backups.size());

                res.add(backup);
            }
        }

        return res;
    }

    /**
     */
    private static class TxMapping {
        /** */
        private final UUID primary;

        /** */
        private final Set<UUID> backups;

        /**
         * @param primary Primary node.
         */
        private TxMapping(ClusterNode primary) {
            this.primary = primary.id();

            backups = new HashSet<>();
        }

        /**
         * @param backup Backup node.
         */
        private void add(ClusterNode backup) {
            backups.add(backup.id());
        }
    }
}