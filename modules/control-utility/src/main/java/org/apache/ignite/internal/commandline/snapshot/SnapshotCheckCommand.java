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
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotPartitionsVerifyTaskResult;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCheckTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCheckTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCheckCommandOption.INCREMENT;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCheckCommandOption.SOURCE;

/**
 * Sub-command to check snapshot.
 */
public class SnapshotCheckCommand extends SnapshotSubcommand {
    /** Default constructor. */
    protected SnapshotCheckCommand() {
        super("check", VisorSnapshotCheckTask.class);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = argIter.nextArg("Expected snapshot name.");
        String snpPath = null;
        Integer incIdx = null;

        while (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg(null);

            SnapshotCheckCommandOption option = CommandArgUtils.of(arg, SnapshotCheckCommandOption.class);

            if (option == null) {
                throw new IllegalArgumentException("Invalid argument: " + arg + ". " +
                    "Possible options: " + F.concat(F.asList(SnapshotCheckCommandOption.values()), ", ") + '.');
            }
            else if (option == SOURCE) {
                if (snpPath != null)
                    throw new IllegalArgumentException(SOURCE.argName() + " arg specified twice.");

                String errMsg = "Expected path to the snapshot directory.";

                if (CommandArgIterator.isCommandOrOption(argIter.peekNextArg()))
                    throw new IllegalArgumentException(errMsg);

                snpPath = argIter.nextArg(errMsg);
            }
            else if (option == INCREMENT) {
                if (incIdx != null)
                    throw new IllegalArgumentException(INCREMENT.argName() + " arg specified twice.");

                String errMsg = "incremental snapshot index.";

                if (CommandArgIterator.isCommandOrOption(argIter.peekNextArg()))
                    throw new IllegalArgumentException("Expected " + errMsg);

                incIdx = argIter.nextIntArg(errMsg);
            }
        }

        cmdArg = new VisorSnapshotCheckTaskArg(snpName, snpPath, incIdx == null ? 0 : incIdx);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = new LinkedHashMap<>(generalUsageOptions());

        params.put(SOURCE.argName() + " " + SOURCE.arg(), SOURCE.description());
        params.put(INCREMENT.argName() + " " + INCREMENT.arg(), INCREMENT.description());

        usage(log, "Check snapshot:", SNAPSHOT, params, name(), SNAPSHOT_NAME_ARG,
            optional(SOURCE.argName(), SOURCE.arg()), optional(INCREMENT.argName(), INCREMENT.arg()));
    }

    /** {@inheritDoc} */
    @Override protected void printResult(Object res, IgniteLogger log) {
        ((SnapshotPartitionsVerifyTaskResult)res).print(log::info);
    }
}
