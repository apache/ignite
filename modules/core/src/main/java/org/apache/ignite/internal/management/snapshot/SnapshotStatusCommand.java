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

package org.apache.ignite.internal.management.snapshot;

import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.SystemViewTask;
import org.apache.ignite.internal.management.api.NoArg;
import org.apache.ignite.internal.util.GridStringBuilder;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T5;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;

/** */
public class SnapshotStatusCommand extends AbstractSnapshotCommand<NoArg> {
    /** {@inheritDoc} */
    @Override public String description() {
        return "Get the status of the current snapshot operation";
    }

    /** {@inheritDoc} */
    @Override public Class<NoArg> argClass() {
        return NoArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotStatusTask> taskClass() {
        return SnapshotStatusTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(NoArg arg, SnapshotTaskResult res0, Consumer<String> printer) {
        Object res;

        try {
            res = res0.result();
        }
        catch (Exception e) {
            throw new IgniteException(e);
        }

        if (res == null) {
            printer.accept("There is no create or restore snapshot operation in progress.");

            return;
        }

        SnapshotStatusTask.SnapshotStatus status = (SnapshotStatusTask.SnapshotStatus)res;

        boolean isCreating = status.operation() == SnapshotStatusTask.SnapshotOperation.CREATE;
        boolean isIncremental = status.incrementIndex() > 0;

        GridStringBuilder s = new GridStringBuilder();

        if (isCreating)
            s.a("Create snapshot operation is in progress.").nl();
        else
            s.a("Restore snapshot operation is in progress.").nl();

        s.a("Snapshot name: ").a(status.name()).nl();
        s.a("Incremental: ").a(isIncremental).nl();

        if (isIncremental)
            s.a("Increment index: ").a(status.incrementIndex()).nl();

        s.a("Operation request ID: ").a(status.requestId()).nl();
        s.a("Started at: ").a(DateFormat.getDateTimeInstance().format(new Date(status.startTime()))).nl();
        s.a("Duration: ").a(X.timeSpan2DHMSM(System.currentTimeMillis() - status.startTime())).nl()
            .nl();
        s.a("Estimated operation progress:").nl();

        printer.accept(s.toString());

        SnapshotTaskProgressDesc desc;

        if (isCreating && isIncremental)
            desc = new CreateIncrementalSnapshotTaskProgressDesc();
        else if (isCreating)
            desc = new CreateFullSnapshotTaskProgressDesc();
        else if (isIncremental)
            desc = new RestoreIncrementalSnapshotTaskProgressDesc();
        else
            desc = new RestoreFullSnapshotTaskProgressDesc();

        List<List<?>> rows = status.progress().entrySet().stream().sorted(Map.Entry.comparingByKey())
            .map(e -> desc.buildRow(e.getKey(), e.getValue()))
            .collect(Collectors.toList());

        SystemViewCommand.printTable(desc.titles(), desc.types(), rows, printer);

        printer.accept(U.nl());
    }

    /** Describes progress of a snapshot task. */
    private abstract static class SnapshotTaskProgressDesc {
        /** Progress table columns titles. */
        private final List<String> titles;

        /** */
        SnapshotTaskProgressDesc(List<String> titles) {
            this.titles = Collections.unmodifiableList(titles);
        }

        /** @return Progress table columns titles. */
        List<String> titles() {
            return titles;
        }

        /** @return Progress table columns types. */
        List<SystemViewTask.SimpleType> types() {
            List<SystemViewTask.SimpleType> types = new ArrayList<>();

            types.add(STRING);

            for (int i = 0; i < titles().size() - 1; i++)
                types.add(NUMBER);

            return types;
        }

        /** @return Progress table data row. */
        abstract List<?> buildRow(UUID nodeId, T5<Long, Long, Long, Long, Long> progress);
    }

    /** */
    private static class CreateFullSnapshotTaskProgressDesc extends SnapshotTaskProgressDesc {
        /** */
        CreateFullSnapshotTaskProgressDesc() {
            super(F.asList("Node ID", "Processed, bytes", "Total, bytes", "Percent"));
        }

        /** {@inheritDoc} */
        @Override public List<?> buildRow(UUID nodeId, T5<Long, Long, Long, Long, Long> progress) {
            long processed = progress.get1();
            long total = progress.get2();

            if (total <= 0)
                return F.asList(nodeId, "unknown", "unknown", "unknown");

            String percent = (int)(processed * 100 / total) + "%";

            return F.asList(nodeId, U.humanReadableByteCount(processed), U.humanReadableByteCount(total), percent);
        }
    }

    /** */
    private static class CreateIncrementalSnapshotTaskProgressDesc extends SnapshotTaskProgressDesc {
        /** */
        CreateIncrementalSnapshotTaskProgressDesc() {
            super(F.asList("Node ID", "Progress"));
        }

        /** {@inheritDoc} */
        @Override public List<?> buildRow(UUID nodeId, T5<Long, Long, Long, Long, Long> progress) {
            return F.asList(nodeId, "unknown");
        }
    }

    /** */
    private static class RestoreFullSnapshotTaskProgressDesc extends SnapshotTaskProgressDesc {
        /** */
        RestoreFullSnapshotTaskProgressDesc() {
            super(F.asList("Node ID", "Processed, partitions", "Total, partitions", "Percent"));
        }

        /** {@inheritDoc} */
        @Override public List<?> buildRow(UUID nodeId, T5<Long, Long, Long, Long, Long> progress) {
            long processed = progress.get1();
            long total = progress.get2();

            if (total <= 0)
                return F.asList(nodeId, "unknown", "unknown", "unknown");

            String percent = (int)(processed * 100 / total) + "%";

            return F.asList(nodeId, processed, total, percent);
        }
    }

    /** */
    private static class RestoreIncrementalSnapshotTaskProgressDesc extends SnapshotTaskProgressDesc {
        /** */
        RestoreIncrementalSnapshotTaskProgressDesc() {
            super(F.asList(
                "Node ID",
                "Processed, partitions",
                "Total, partitions",
                "Percent",
                "Processed, WAL segments",
                "Total, WAL segments",
                "Percent",
                "Processed, WAL entries"));
        }

        /** {@inheritDoc} */
        @Override public List<?> buildRow(UUID nodeId, T5<Long, Long, Long, Long, Long> progress) {
            List<Object> result = new ArrayList<>();
            result.add(nodeId);

            long processedParts = progress.get1();
            long totalParts = progress.get2();

            if (totalParts <= 0)
                result.addAll(F.asList("unknown", "unknown", "unknown"));
            else {
                String partsPercent = (int)(processedParts * 100 / totalParts) + "%";

                result.add(F.asList(processedParts, totalParts, partsPercent));
            }

            long processedWalSegs = progress.get3();
            long totalWalSegs = progress.get4();

            if (processedWalSegs <= 0)
                result.addAll(F.asList("unknown", "unknown", "unknown"));
            else {
                String walSegsPercent = (int)(processedWalSegs * 100 / totalWalSegs) + "%";

                result.add(F.asList(processedWalSegs, totalWalSegs, walSegsPercent));
            }

            long processedWalEntries = progress.get5();

            if (processedWalEntries <= 0)
                result.add("unknown");
            else
                result.add(processedWalEntries);

            return result;
        }
    }
}
