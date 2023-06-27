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

package org.apache.ignite.internal.management.cache;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusInfoContainer;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusTask;

/** Index rebuild status. */
public class CacheIndexesRebuildStatusCommand
    implements ComputeCommand<CacheIndexesRebuildStatusCommandArg, Map<UUID, Set<IndexRebuildStatusInfoContainer>>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "List all indexes that have index rebuild in progress";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheIndexesRebuildStatusCommandArg> argClass() {
        return CacheIndexesRebuildStatusCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IndexRebuildStatusTask> taskClass() {
        return IndexRebuildStatusTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, CacheIndexesRebuildStatusCommandArg arg) {
        return CommandUtils.nodeOrAll(arg.nodeId(), nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheIndexesRebuildStatusCommandArg arg,
        Map<UUID, Set<IndexRebuildStatusInfoContainer>> res,
        Consumer<String> printer
    ) {
        if (res.isEmpty()) {
            printer.accept("There are no caches that have index rebuilding in progress.");
            printer.accept("");

            return;
        }

        printer.accept("Caches that have index rebuilding in progress:");

        for (Map.Entry<UUID, Set<IndexRebuildStatusInfoContainer>> entry: res.entrySet()) {
            printer.accept("");

            entry.getValue().stream()
                .sorted(IndexRebuildStatusInfoContainer.comparator())
                .forEach(container -> printer.accept(constructCacheOutputString(entry.getKey(), container)));
        }

        printer.accept("");
    }

    /** */
    private String constructCacheOutputString(UUID nodeId, IndexRebuildStatusInfoContainer container) {
        return "node_id=" + nodeId + ", " + container.toString();
    }
}
