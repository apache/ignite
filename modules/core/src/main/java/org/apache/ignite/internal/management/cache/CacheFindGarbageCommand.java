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

import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;

import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;

/** Find and remove garbage. */
public class CacheFindGarbageCommand
    implements ComputeCommand<CacheFindGarbageCommandArg, FindAndDeleteGarbageInPersistenceTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Find and optionally delete garbage from shared cache groups which could be left after cache destroy";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheFindGarbageCommandArg> argClass() {
        return CacheFindGarbageCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<FindAndDeleteGarbageInPersistenceTask> taskClass() {
        return FindAndDeleteGarbageInPersistenceTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheFindGarbageCommandArg arg,
        FindAndDeleteGarbageInPersistenceTaskResult res,
        Consumer<String> printer
    ) {
        CommandUtils.printErrors(res.exceptions(), "Scanning for garbage failed on nodes:", printer);

        for (Map.Entry<UUID, FindAndDeleteGarbageInPersistenceJobResult> nodeEntry : res.result().entrySet()) {
            if (!nodeEntry.getValue().hasGarbage()) {
                printer.accept("Node " + nodeEntry.getKey() + " - garbage not found.");

                continue;
            }

            printer.accept("Garbage found on node " + nodeEntry.getKey() + ":");

            FindAndDeleteGarbageInPersistenceJobResult value = nodeEntry.getValue();

            Map<Integer, Map<Integer, Long>> grpPartErrorsCount = value.checkResult();

            if (!grpPartErrorsCount.isEmpty()) {
                for (Map.Entry<Integer, Map<Integer, Long>> entry : grpPartErrorsCount.entrySet()) {
                    for (Map.Entry<Integer, Long> e : entry.getValue().entrySet()) {
                        printer.accept(INDENT + "Group=" + entry.getKey() +
                            ", partition=" + e.getKey() +
                            ", count of keys=" + e.getValue());
                    }
                }
            }

            printer.accept("");
        }
    }
}
