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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.commandline.cache.check_indexes_inline_size.CheckIndexInlineSizesResult;
import org.apache.ignite.internal.commandline.cache.check_indexes_inline_size.CheckIndexInlineSizesTask;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.servers;

/** Check secondary indexes inline size. */
public class CacheCheckIndexInlineSizesCommand
    implements ComputeCommand<NoArg, CheckIndexInlineSizesResult> {
    /** Success message. */
    public static final String INDEXES_INLINE_SIZE_ARE_THE_SAME =
        "All secondary indexes have the same effective inline size on all cluster nodes.";

    /** {@inheritDoc} */
    @Override public String description() {
        return "Checks that secondary indexes inline size are same on the cluster nodes";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<CheckIndexInlineSizesTask> taskClass() {
        return CheckIndexInlineSizesTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Collection<GridClientNode> nodes, NoArg arg) {
        return servers(nodes);
    }

    /** {@inheritDoc} */
    @Override public void printResult(NoArg arg, CheckIndexInlineSizesResult res, Consumer<String> printer) {
        Map<String, Map<Integer, Set<UUID>>> indexToSizeNode = new HashMap<>();

        for (Map.Entry<UUID, Map<String, Integer>> nodeRes : res.inlineSizes().entrySet()) {
            for (Map.Entry<String, Integer> index : nodeRes.getValue().entrySet()) {
                Map<Integer, Set<UUID>> sizeToNodes = indexToSizeNode.computeIfAbsent(index.getKey(), x -> new HashMap<>());

                sizeToNodes.computeIfAbsent(index.getValue(), x -> new HashSet<>()).add(nodeRes.getKey());
            }
        }

        printer.accept("Found " + indexToSizeNode.size() + " secondary indexes.");

        Map<String, Map<Integer, Set<UUID>>> problems = indexToSizeNode.entrySet().stream()
            .filter(e -> e.getValue().size() > 1)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (F.isEmpty(problems))
            printer.accept(INDEXES_INLINE_SIZE_ARE_THE_SAME);
        else
            printProblemsAndShowRecommendations(problems, printer);
    }

    /** */
    private void printProblemsAndShowRecommendations(Map<String, Map<Integer, Set<UUID>>> problems, Consumer<String> printer) {
        printer.accept(problems.size() +
            " index(es) have different effective inline size on nodes. It can lead to performance degradation in SQL queries.");
        printer.accept("Index(es):");

        for (Map.Entry<String, Map<Integer, Set<UUID>>> entry : problems.entrySet()) {
            SB sb = new SB();

            sb.a("Full index name: ").a(entry.getKey());

            for (Integer size : entry.getValue().keySet())
                sb.a(" nodes: ").a(entry.getValue().get(size)).a(" inline size: ").a(size).a(",");

            sb.setLength(sb.length() - 1);

            printer.accept(INDENT + sb);
        }

        printer.accept("");

        printer.accept("Recommendations:");
        printer.accept(
            INDENT + "Check that value of property " + IgniteSystemProperties.IGNITE_MAX_INDEX_PAYLOAD_SIZE + " are the same on all nodes."
        );
        printer.accept(INDENT + "Recreate indexes (execute DROP INDEX, CREATE INDEX commands) with different inline size.");
    }
}
