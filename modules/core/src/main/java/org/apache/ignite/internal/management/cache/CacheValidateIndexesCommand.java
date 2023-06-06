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

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.management.api.CommandUtils;
import org.apache.ignite.internal.management.api.ComputeCommand;
import org.apache.ignite.internal.processors.cache.verify.PartitionKey;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.internal.visor.verify.IndexIntegrityCheckIssue;
import org.apache.ignite.internal.visor.verify.IndexValidationIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeIssue;
import org.apache.ignite.internal.visor.verify.ValidateIndexesCheckSizeResult;
import org.apache.ignite.internal.visor.verify.ValidateIndexesPartitionResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesJobResult;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTask;
import org.apache.ignite.internal.visor.verify.VisorValidateIndexesTaskResult;
import static org.apache.ignite.internal.management.api.CommandUtils.DOUBLE_INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.INDENT;
import static org.apache.ignite.internal.management.api.CommandUtils.join;

/** Validates indexes attempting to read each indexed entry. */
public class CacheValidateIndexesCommand
    implements ComputeCommand<CacheValidateIndexesCommandArg, VisorValidateIndexesTaskResult> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Validates indexes for the specified caches/cache groups on an idle cluster " +
            "on all or specified cluster nodes. validate_indexes checks consistence between primary/secondary " +
            "indexes against each other and data entries";
    }

    /** {@inheritDoc} */
    @Override public Class<CacheValidateIndexesCommandArg> argClass() {
        return CacheValidateIndexesCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorValidateIndexesTask> taskClass() {
        return VisorValidateIndexesTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Map<UUID, T3<Boolean, Object, Long>> nodes, CacheValidateIndexesCommandArg arg) {
        return arg.nodeIds() != null ? Arrays.asList(arg.nodeIds()) : null;
    }

    /** {@inheritDoc} */
    @Override public void printResult(
        CacheValidateIndexesCommandArg arg,
        VisorValidateIndexesTaskResult res0,
        Consumer<String> printer
    ) {
        boolean errors = CommandUtils.printErrors(res0.exceptions(), "Index validation failed on nodes:", printer);

        for (Map.Entry<UUID, VisorValidateIndexesJobResult> nodeEntry : res0.results().entrySet()) {
            VisorValidateIndexesJobResult jobRes = nodeEntry.getValue();

            if (!jobRes.hasIssues())
                continue;

            errors = true;

            printer.accept("Index issues found on node " + nodeEntry.getKey() + ":");

            for (IndexIntegrityCheckIssue is : jobRes.integrityCheckFailures())
                printer.accept(INDENT + is);

            for (Map.Entry<PartitionKey, ValidateIndexesPartitionResult> e : jobRes.partitionResult().entrySet()) {
                ValidateIndexesPartitionResult res = e.getValue();

                if (!res.issues().isEmpty()) {
                    printer.accept(INDENT + join(" ", e.getKey(), e.getValue()));

                    for (IndexValidationIssue is : res.issues())
                        printer.accept(DOUBLE_INDENT + is);
                }
            }

            for (Map.Entry<String, ValidateIndexesPartitionResult> e : jobRes.indexResult().entrySet()) {
                ValidateIndexesPartitionResult res = e.getValue();

                if (!res.issues().isEmpty()) {
                    printer.accept(INDENT + join(" ", "SQL Index", e.getKey(), e.getValue()));

                    for (IndexValidationIssue is : res.issues())
                        printer.accept(DOUBLE_INDENT + is);
                }
            }

            for (Map.Entry<String, ValidateIndexesCheckSizeResult> e : jobRes.checkSizeResult().entrySet()) {
                ValidateIndexesCheckSizeResult res = e.getValue();
                Collection<ValidateIndexesCheckSizeIssue> issues = res.issues();

                if (issues.isEmpty())
                    continue;

                printer.accept(INDENT + join(" ", "Size check", e.getKey(), res));

                for (ValidateIndexesCheckSizeIssue issue : issues)
                    printer.accept(DOUBLE_INDENT + issue);
            }
        }

        if (!errors)
            printer.accept("no issues found.");
        else
            printer.accept("issues found (listed above).");

        printer.accept("");
    }
}
