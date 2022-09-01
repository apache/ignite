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

import java.util.UUID;
import java.util.logging.Logger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotCancelTask;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;

/**
 * Sub-command to cancel running snapshot.
 */
public class SnapshotCancelCommand extends SnapshotSubcommand {

    /** Default constructor. */
    protected SnapshotCancelCommand() {
        super("cancel", VisorSnapshotCancelTask.class);
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        cmdArg = UUID.fromString(argIter.nextArg("Expected snapshot operation ID."));

        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument: " + argIter.peekNextArg() + '.');
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        usage(log, "Cancel running snapshot operation:", SNAPSHOT,
            F.asMap("operationId", "Snapshot operation ID."), name(), "operationId");
    }
}
