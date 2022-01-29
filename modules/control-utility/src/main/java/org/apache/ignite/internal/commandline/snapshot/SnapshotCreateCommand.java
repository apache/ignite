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
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCreateTaskArg;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.CommandLogger.optional;

/**
 * Snapshot create sub-command.
 */
public class SnapshotCreateCommand extends SnapshotSubcommand {
    /** Default contructor. */
    protected SnapshotCreateCommand() {
        super("create", VisorSnapshotCreateTask.class);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        String snpName = argIter.nextArg("Expected snapshot name.");
        boolean sync = false;

        if (argIter.hasNextSubArg()) {
            String arg = argIter.nextArg("");

            if (!"--sync".equals(arg))
                throw new IllegalArgumentException("Invalid argument: " + arg + '.');

            sync = true;
        }

        cmdArg = new VisorSnapshotCreateTaskArg(snpName, sync);
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        Map<String, String> params = new LinkedHashMap<String, String>() {{
            put("snapshot_name", "Snapshot name.");
            put("sync", "Run the operation synchronously, the command will wait for the entire operation to complete. " +
                    "Otherwise, the it will be performed in the background, and the command will immediately return control.");
        }};

        usage(log, "Create cluster snapshot:", SNAPSHOT, params, name, "snapshot_name", optional("--sync"));
    }
}
