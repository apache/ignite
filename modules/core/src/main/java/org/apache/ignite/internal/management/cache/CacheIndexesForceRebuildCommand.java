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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.cache.index.IndexForceRebuildTask;
import org.apache.ignite.internal.visor.cache.index.IndexForceRebuildTaskRes;
import org.apache.ignite.internal.visor.cache.index.IndexRebuildStatusInfoContainer;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** Index force rebuild. */
public class CacheIndexesForceRebuildCommand implements ComputeCommand<CacheIndexesForceRebuildCommandArg, IndexForceRebuildTaskRes> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Triggers rebuild of all indexes for specified caches or cache groups";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheIndexesForceRebuildCommandArg> argClass() {
        return CacheIndexesForceRebuildCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<IndexForceRebuildTask> taskClass() {
        return IndexForceRebuildTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T3<Boolean, Object, Long>> nodes, CacheIndexesForceRebuildCommandArg arg) {
        if (arg.nodeId() != null) {
            T3<Boolean, Object, Long> nodeDesc = nodes.get(arg.nodeId());

            if (nodeDesc != null && nodeDesc.get1())
                throw new IllegalArgumentException("Please, specify server node id");

            return Collections.singleton(arg.nodeId());
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheIndexesForceRebuildCommandArg arg,
        IndexForceRebuildTaskRes res,
        Consumer<String> printer
    ) {
        if (!F.isEmpty(res.notFoundCacheNames())) {
            String warning = arg.groupNames() == null ?
                "WARNING: These caches were not found:" : "WARNING: These cache groups were not found:";

            printer.accept(warning);

            res.notFoundCacheNames()
                .stream()
                .sorted()
                .forEach(name -> printer.accept(INDENT + name));

            printer.accept("");
        }

        if (!F.isEmpty(res.cachesWithRebuildInProgress())) {
            printer.accept("WARNING: These caches have indexes rebuilding in progress:");

            printInfos(res.cachesWithRebuildInProgress(), printer);

            printer.accept("");
        }

        if (!F.isEmpty(res.cachesWithStartedRebuild())) {
            printer.accept("Indexes rebuild was started for these caches:");

            printInfos(res.cachesWithStartedRebuild(), printer);
        }
        else
            printer.accept("WARNING: Indexes rebuild was not started for any cache. Check command input.");

        printer.accept("");
    }

    /** */
    private void printInfos(Collection<IndexRebuildStatusInfoContainer> infos, Consumer<String> printer) {
        infos.stream()
            .sorted(IndexRebuildStatusInfoContainer.comparator())
            .forEach(rebuildStatusInfo -> printer.accept(INDENT + rebuildStatusInfo.toString()));
    }
}
