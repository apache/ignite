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
import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.argument.CommandArgUtils;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCreateCommandOption.SYNC;

/**
 * Snapshot create sub-command.
 */
public class SnapshotCreateCommand extends SnapshotSubcommand {
    /** Default constructor. */
    protected SnapshotCreateCommand() {
        super("create", VisorSnapshotCreateTask.class);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = argIter.nextArg("Expected snapshot name.");
        boolean sync = false;

        if (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg(null);

            SnapshotCreateCommandOption option = CommandArgUtils.of(arg, SnapshotCreateCommandOption.class);

            if (option == null)
                throw new IllegalArgumentException("Invalid argument: " + arg + ". ");

            assert option == SYNC;

            sync = true;
        }

        cmdArg = new VisorSnapshotCreateTaskArg(snpName, sync);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new LinkedHashMap<>(generalUsageOptions());

        params.put(SYNC.optionName(), SYNC.description());

        usage(log, "Create cluster snapshot:", SNAPSHOT, params, name(), SNAPSHOT_NAME_ARG, optional(SYNC.argName()));
    }
}
