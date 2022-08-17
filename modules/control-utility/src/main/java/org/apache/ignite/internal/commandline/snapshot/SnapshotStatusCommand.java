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
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.commandline.AbstractCommand;
import org.apache.ignite.internal.commandline.Command;
import org.apache.ignite.internal.commandline.systemview.SystemViewCommand;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotStatusTask;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotStatusTask.SnapshotStatus;
import org.apache.ignite.internal.visor.snapshot.VisorSnapshotTaskResult;

import static org.apache.ignite.internal.commandline.CommandList.SNAPSHOT;
import static org.apache.ignite.internal.commandline.TaskExecutor.executeTask;
import static org.apache.ignite.internal.commandline.snapshot.SnapshotSubcommands.STATUS;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/**
 * Command to get the status of a current snapshot operation in the cluster.
 */
public class SnapshotStatusCommand extends AbstractCommand<Object> {
    /** {@inheritDoc} */
    @Override public Object execute(GridClientConfiguration clientCfg, Logger log) throws Exception {
        try (GridClient client = Command.startClient(clientCfg)) {
            VisorSnapshotTaskResult res = executeTask(
                client,
                VisorSnapshotStatusTask.class,
                null,
                clientCfg
            );

            printStatus((SnapshotStatus)res.result(), log);

            return res;
        }
    }

    /** Prints the snapshot operation status to the log. */
    private void printStatus(SnapshotStatus status, Logger log) {
        if (status == null) {
            log.info("There is no create or restore snapshot operation in progress.");

            return;
        }

        boolean isCreating = status.operation() == VisorSnapshotStatusTask.SnapshotOperation.CREATE;

        GridStringBuilder s = new GridStringBuilder();

        if (isCreating)
            s.a("Create snapshot operation is in progress.").nl();
        else
            s.a("Restore snapshot operation is in progress.").nl();

        s.a("Snapshot name: ").a(status.name()).nl();
        s.a("Operation ID: ").a(status.requestId()).nl();
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

        SystemViewCommand.printTable(titles, F.asList(STRING, STRING, STRING, STRING),
            rows, log);

        log.info(U.nl());
    }

    /** {@inheritDoc} */
    @Override public Object arg() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void printUsage(Logger log) {
        usage(log, "Get the status of a current snapshot operation:", SNAPSHOT, STATUS.toString());
    }

    /** {@inheritDoc} */
    @Override public String name() {
        return STATUS.name();
    }
}
