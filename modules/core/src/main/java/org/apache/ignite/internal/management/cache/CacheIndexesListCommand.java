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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.visor.cache.index.IndexListInfoContainer;
import org.apache.ignite.internal.visor.cache.index.IndexListTask;

/** Index list. */
public class CacheIndexesListCommand implements ComputeCommand<CacheIndexesListCommandArg, Set<IndexListInfoContainer>> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "List all indexes that match specified filters";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheIndexesListCommandArg> argClass() {
        return CacheIndexesListCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IndexListTask> taskClass() {
        return IndexListTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, GridClientNode> nodes, CacheIndexesListCommandArg arg) {
        return CommandUtils.nodeOrNull(arg.nodeId());
    }

    /** {@inheritDoc} */
    @Override public void printResult(CacheIndexesListCommandArg arg, Set<IndexListInfoContainer> res, Consumer<String> printer) {
        List<IndexListInfoContainer> sorted = new ArrayList<>(res);

        sorted.sort(IndexListInfoContainer.comparator());

        String prevGrpName = "";

        for (IndexListInfoContainer container : sorted) {
            if (!prevGrpName.equals(container.groupName())) {
                prevGrpName = container.groupName();

                printer.accept("");
            }

            printer.accept(container.toString());
        }

        printer.accept("");
    }
}
