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

package org.apache.ignite.internal.management.consistency;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.management.api.ExperimentalCommand;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.visor.consistency.VisorConsistencyTaskResult;

/** */
public class ConsistencyRepairCommand implements
    ExperimentalCommand<ConsistencyRepairCommandArg, VisorConsistencyTaskResult>,
    ComputeCommand<ConsistencyRepairCommandArg, VisorConsistencyTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Check/Repair cache consistency using Read Repair approach";
    }

    /** {@inheritDoc} */
    @Override public Class<ConsistencyRepairCommandArg> argClass() {
        return ConsistencyRepairCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class taskClass() {
        return null; //VisorConsistencyRepairTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T2<Boolean, Object>> nodes, ConsistencyRepairCommandArg arg) {
        return arg.parallel()
            ? nodes.keySet()
            : nodes.entrySet().stream()
                .filter(e -> !e.getValue().get1())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
}
