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

package org.apache.ignite.internal.management.defragmentation;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.defragmentation.DefragmentationCommand.DefragmentationStatusCommandArg;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationTask;
import org.apache.ignite.internal.visor.defragmentation.VisorDefragmentationTaskResult;

/** */
public class DefragmentationScheduleCommand
    implements ComputeCommand<DefragmentationStatusCommandArg, VisorDefragmentationTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Schedule PDS defragmentation";
    }

    /** {@inheritDoc} */
    @Override public Class<DefragmentationScheduleCommandArg> argClass() {
        return DefragmentationScheduleCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorDefragmentationTask> taskClass() {
        return VisorDefragmentationTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        DefragmentationStatusCommandArg arg,
        VisorDefragmentationTaskResult res,
        Consumer<String> printer
    ) {
        printer.accept(res.getMessage());
    }

    /** {@inheritDoc} */
    @Override public Collection<GridClientNode> nodes(Map<UUID, GridClientNode> nodes, DefragmentationStatusCommandArg arg0) {
        DefragmentationScheduleCommandArg arg = (DefragmentationScheduleCommandArg)arg0;

        if (F.isEmpty(arg.nodes()))
            return null;

        Set<String> nodesArg = new HashSet<>(Arrays.asList(arg.nodes()));

        return nodes.entrySet().stream()
            .filter(e -> nodesArg.contains(Objects.toString(e.getValue().consistentId())))
            .map(Map.Entry::getValue)
            .collect(Collectors.toList());
    }
}
