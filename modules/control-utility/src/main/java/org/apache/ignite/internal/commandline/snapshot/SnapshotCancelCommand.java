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

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCancelTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCancelTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.or;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCancelCommandOption.ID;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotCancelCommandOption.NAME;

/**
 * Sub-command to cancel running snapshot.
 */
public class SnapshotCancelCommand extends SnapshotSubcommand {
    /** Default constructor. */
    protected SnapshotCancelCommand() {
        super("cancel", VisorSnapshotCancelTask.class);
    }

    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, IgniteLogger log) throws Exception {
        if (!F.isEmpty(((VisorSnapshotCancelTaskArg)cmdArg).snapshotName()))
            log.warning("'" + NAME.arg() + "' option is deprecated, please use operation request ID to cancel operation.");

        return super.execute(clientCfg, log);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        UUID reqId = null;
        String snpName = null;
        String explainMsg = "One of " + Arrays.toString(SnapshotCancelCommandOption.values()) + " is expected.";

        String arg = argIter.nextArg(explainMsg);

        if (arg.equals(ID.argName()))
            reqId = UUID.fromString(argIter.nextArg("Expected operation request ID."));
        else if (arg.equals(NAME.argName()))
            snpName = argIter.nextArg("Expected snapshot name.");
        else
            throw new IllegalArgumentException("Unexpected argument: " + argIter.peekNextArg() + ". " + explainMsg);

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("No more arguments expected.");

        cmdArg = new VisorSnapshotCancelTaskArg(reqId, snpName);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        Map<String, String> params = new LinkedHashMap<>();

        params.put(ID.argName() + " " + ID.arg(), ID.description());
        params.put(NAME.argName() + " " + NAME.arg(), NAME.description());

        usage(log, "Cancel running snapshot operation:", SNAPSHOT,
            params, name(), or(ID.argName() + " " + ID.arg(), NAME.argName() + " " + NAME.arg()));
    }
}
