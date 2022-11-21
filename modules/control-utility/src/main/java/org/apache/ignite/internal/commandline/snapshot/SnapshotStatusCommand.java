/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.commandline.snapshot;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.commandline.CommandArgIterator;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotStatusTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotStatusTask.SnapshotStatus;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommands.STATUS;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/**
 * Command to get the status of the current snapshot operation in the cluster.
 */
public class SnapshotStatusCommand extends SnapshotSubcommand {
    /** */
    protected SnapshotStatusCommand() {
        super("status", VisorSnapshotStatusTask.class);
    }

    /** {@inheritDoc} */
    @Override protected void printResult(Object res, IgniteLogger log) {
        if (res == null) {
            log.info("There is no create or restore snapshot operation in progress.");

            return;
        }

        SnapshotStatus status = (SnapshotStatus)res;

        boolean isCreating = status.operation() == VisorSnapshotStatusTask.SnapshotOperation.CREATE;

        GridStringBuilder s = new GridStringBuilder();

        if (isCreating)
            s.a("Create snapshot operation is in progress.").nl();
        else
            s.a("Restore snapshot operation is in progress.").nl();

        s.a("Snapshot name: ").a(status.name()).nl();
        s.a("Operation request ID: ").a(status.requestId()).nl();
        s.a("Started at: ").a(DateFormat.getDateTimeInstance().format(new Date(status.startTime()))).nl();
        s.a("Duration: ").a(X.timeSpan2DHMSM(System.currentTimeMillis() - status.startTime())).nl()
                .nl();
        s.a("Estimated operation progress:").nl();

        log.info(s.toString());

        List<String> titles = isCreating ? F.asList("Node ID", "Processed, bytes", "Total, bytes", "Percent") :
                F.asList("Node ID", "Processed, partitions", "Total, partitions", "Percent");

        List<List<?>> rows = status.progress().entrySet().stream().sorted(Map.Entry.comparingByKey()).map(e -> {
            UUID nodeId = e.getKey();
            long processed = e.getValue().get1();
            long total = e.getValue().get2();

            if (total <= 0)
                return F.asList(nodeId, "unknown", "unknown", "unknown");

            String percent = (int)(processed * 100 / total) + "%";

            if (isCreating)
                return F.asList(nodeId, U.humanReadableByteCount(processed), U.humanReadableByteCount(total), percent);
            else
                return F.asList(nodeId, processed, total, percent);
        }).collect(Collectors.toList());

        SystemViewCommand.printTable(titles, F.asList(STRING, NUMBER, NUMBER, NUMBER),
                rows, log);

        log.info(U.nl());
    }

    /** {@inheritDoc} */
    @Override public void parseArguments(CommandArgIterator argIter) {
        if (argIter.hasNextSubArg())
            throw new IllegalArgumentException("Unexpected argument: " + argIter.peekNextArg() + '.');
    }

    /** {@inheritDoc} */
    @Override public void printUsage(IgniteLogger log) {
        usage(log, "Get the status of the current snapshot operation:", SNAPSHOT, STATUS.toString());
    }
}
