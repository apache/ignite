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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.management.SystemViewCommand;
import org.apache.ignite.internal.management.SystemViewTask;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListJobResult;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.SnapshotListTaskResult;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.DATE;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.management.SystemViewTask.SimpleType.STRING;

/** Snapshot list command. */
public class SnapshotListCommand extends AbstractSnapshotCommand<SnapshotListCommandArg, SnapshotListTaskResult> {
    /** */
    public static final String HEADER = "Snapshots were found on the following nodes:";

    /** */
    public static final String DESC = "Lists all snapshots on all online server nodes, including their sizes";

    /** */
    public static final String NO_SNAPSHOTS = "No snapshots found.";

    /** */
    public static final String EXCEPTION_PREF = "ERROR: ";

    /** {@inheritDoc} */
    @Override public String description() {
        return DESC;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListCommandArg> argClass() {
        return SnapshotListCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<SnapshotListTask> taskClass() {
        return SnapshotListTask.class;
    }

    /** {@inheritDoc} */
    @Override public void printResult(SnapshotListCommandArg arg, SnapshotListTaskResult res, Consumer<String> printer) {
        printer.accept(HEADER);

        // Sorting the results by consistent id for better reading.
        Integer[] nodesOrder = IntStream.range(0, res.nodesIds().length).boxed()
            .sorted(Comparator.comparing(idx -> res.consistentIds()[idx]))
            .toArray(Integer[]::new);

        // Flag whether the node's result has at least one incremental snapshot and one external storage.
        T2<Boolean, Boolean> incsAndExtStors = new T2<>();

        for (int nodeIdx : nodesOrder) {
            printer.accept("");

            printer.accept("Node '%s' [uuid=%s]:".formatted(res.consistentIds()[nodeIdx], res.nodesIds()[nodeIdx]));

            IgniteException nodeErr = res.errors() == null || res.errors()[nodeIdx] == null ? null : res.errors()[nodeIdx];

            if (nodeErr != null) {
                Throwable cause = X.cause(nodeErr, IgniteCheckedException.class);

                if (cause == null)
                    cause = nodeErr;

                printer.accept(EXCEPTION_PREF + "%s [errorType=%s.class]"
                    .formatted(cause.getMessage(), cause.getClass().getSimpleName()));

                continue;
            }

            SnapshotListJobResult nodeResult = res.nodesSnapshots()[nodeIdx];

            if (nodeResult.snapshots().isEmpty()) {
                printer.accept(NO_SNAPSHOTS);

                continue;
            }

            Map<String, SnapshotListJobResult.SnapshotInfo> perNodeResults = prepareResults(nodeResult.snapshots(), incsAndExtStors);

            SystemViewCommand.printTable(
                perNodeTableTitles(incsAndExtStors.get1(), incsAndExtStors.get2()),
                perNodeTableTypes(incsAndExtStors.get1(), incsAndExtStors.get2()),
                perNodeTableRows(perNodeResults, incsAndExtStors.get1(), incsAndExtStors.get2()),
                printer
            );
        }

        printer.accept("");
    }

    /** */
    private static List<String> perNodeTableTitles(boolean hasIncrementals, boolean hasExtStorages) {
        List<String> res = new ArrayList<>(7);

        res.addAll(F.asList("Name", "Total size", "Creation date"));

        if (hasIncrementals)
            res.addAll(F.asList("Last incremental date", "Incremental parts", "Incremental parts size"));

        if (hasExtStorages)
            res.addAll(F.asList("External storages size"));

        return res;
    }

    /** */
    private static List<SystemViewTask.SimpleType> perNodeTableTypes(boolean hasIncrementals, boolean hasExtStorages) {
        List<SystemViewTask.SimpleType> res = new ArrayList<>(7);

        // Main columns: name, size (formatted string), creation date (formatted string).
        res.addAll(F.asList(STRING, STRING, DATE));

        // Optional columns of incremental parts: last modification date, number of incremental parts, total size (formatted string).
        if (hasIncrementals)
            res.addAll(F.asList(DATE, NUMBER, STRING));

        // Optional column of external storages: total size (formatted string).
        if (hasExtStorages)
            res.add(STRING);

        return res;
    }

    /** */
    private static List<List<?>> perNodeTableRows(
        Map<String, SnapshotListJobResult.SnapshotInfo> results,
        boolean includeIncrementals,
        boolean includeExtStorages
    ) {
        List<List<?>> table = new ArrayList<>(results.size());

        results.forEach((name, s) -> {
            List<Object> row = new ArrayList<>(7);

            // Main columns: name, size (formatted string), creation date (formatted string).
            row.addAll(F.asList(name, U.humanReadableByteCount(s.size()), Date.from(Instant.ofEpochMilli(s.date()))));

            if (includeIncrementals) {
                // Optional columns of incremental parts: last modification date, count, total size (formatted string).
                SnapshotListJobResult.SnapshotInfo inc = s.incrementals();

                // Empty strings instead of nulls to avoid 'null' in empty table cells.
                row.addAll(F.asList(
                    inc == null ? "" : Date.from(Instant.ofEpochMilli(inc.date())), // Modification date.
                    inc == null ? "" : inc.number(), // Count.
                    inc == null ? "" : U.humanReadableByteCount(inc.size()) // Total size.
                ));
            }

            if (includeExtStorages) {
                // Optional column of external storages size.
                SnapshotListJobResult.SnapshotInfo ext = s.externalStorages();

                row.add(ext == null ? "" : U.humanReadableByteCount(ext.size())); // Total size of all external storages.
            }

            table.add(row);
        });

        return table;
    }

    /** Prepares the results: sorts, finds optional data to hide if it doesn't present. */
    private static Map<String, SnapshotListJobResult.SnapshotInfo> prepareResults(
        Map<String, SnapshotListJobResult.SnapshotInfo> rawResults,
        T2<Boolean, Boolean> hasIncsAndExtStrs
    ) {
        Comparator<SnapshotListJobResult.SnapshotInfo> cmp = new Comparator<>() {
            @Override public int compare(SnapshotListJobResult.SnapshotInfo s1, SnapshotListJobResult.SnapshotInfo s2) {
                long date1 = s1.incrementals() == null ? s1.date() : s1.incrementals().date();
                long date2 = s2.incrementals() == null ? s2.date() : s2.incrementals().date();

                return Long.compare(date2, date1);
            }
        };

        hasIncsAndExtStrs.set1(false);
        hasIncsAndExtStrs.set2(false);

        // Sorted by the creation or modification date.
        return rawResults.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue(cmp))
            // Always-accepting filter: sets the optional-column flags while collecting, in one pass.
            .filter(e -> {
                if (e.getValue().incrementals() != null)
                    hasIncsAndExtStrs.set1(true);

                if (e.getValue().externalStorages() != null)
                    hasIncsAndExtStrs.set2(true);

                return true;
            })
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (e1, e2) -> e1,
                LinkedHashMap::new
            ));
    }
}
