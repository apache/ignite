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

package org.apache.ignite.internal.commandline.snapshot;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCreateCommandOption.DESTINATION;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCreateCommandOption.INCREMENTAL;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCreateCommandOption.ONLY_PRIMARY;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCreateCommandOption.SYNC;

/**
 * Sub-command to create a cluster snapshot.
 */
public class SnapshotCreateCommand extends SnapshotSubcommand {
    /** Default constructor. */
    protected SnapshotCreateCommand() {
        super("create", VisorSnapshotCreateTask.class);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = argIter.nextArg("Expected snapshot name.");
        String snpPath = null;
        boolean sync = false;
        boolean incremental = false;
        boolean onlyPrimary = false;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg(null);

            SnapshotCreateCommandOption option = CommandArgUtils.of(arg, SnapshotCreateCommandOption.class);

            if (option == null) {
                throw new IllegalArgumentException("Invalid argument: " + arg + ". " +
                    "Possible options: " + F.concat(F.asList(SnapshotCreateCommandOption.values()), ", ") + '.');
            }
            else if (option == DESTINATION) {
                if (snpPath != null)
                    throw new IllegalArgumentException(DESTINATION.argName() + " arg specified twice.");

                String errMsg = "Expected path to the snapshot directory.";

                if (CommandArgIterator.isCommandOrOption(argIter.peekNextArg()))
                    throw new IllegalArgumentException(errMsg);

                snpPath = argIter.nextArg(errMsg);
            }
            else if (option == SYNC) {
                if (sync)
                    throw new IllegalArgumentException(SYNC.argName() + " arg specified twice.");

                sync = true;
            }
            else if (option == INCREMENTAL) {
                if (incremental)
                    throw new IllegalArgumentException(INCREMENTAL.argName() + " arg specified twice.");

                incremental = true;
            }
            else if (option == ONLY_PRIMARY) {
                if (onlyPrimary)
                    throw new IllegalArgumentException(ONLY_PRIMARY.argName() + " arg specified twice.");

                onlyPrimary = true;
            }
        }

        if (onlyPrimary && incremental)
            throw new IllegalArgumentException(ONLY_PRIMARY.argName() + " not supported for incremental snapshots.");

        cmdArg = new VisorSnapshotCreateTaskArg(snpName, snpPath, sync, incremental, onlyPrimary);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = new LinkedHashMap<>(generalUsageOptions());

        params.put(ONLY_PRIMARY.argName(), ONLY_PRIMARY.description());
        params.put(DESTINATION.argName() + " " + DESTINATION.arg(), DESTINATION.description());
        params.put(SYNC.argName(), SYNC.description());
        params.put(INCREMENTAL.argName(), INCREMENTAL.description());

        usage(log, "Create cluster snapshot:", SNAPSHOT, params, name(), SNAPSHOT_NAME_ARG,
            optional(ONLY_PRIMARY.argName()), optional(DESTINATION.argName(), DESTINATION.arg()),
            optional(SYNC.argName()), optional(INCREMENTAL.argName()));
    }
}
