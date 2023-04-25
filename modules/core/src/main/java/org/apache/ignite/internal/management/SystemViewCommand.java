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

package org.apache.ignite.internal.management;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.ignite.internal.management.api.Command;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTask;
import org.apache.ignite.internal.visor.systemview.VisorSystemViewTaskResult;
import static java.util.Collections.nCopies;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.DATE;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.NUMBER;
import static org.apache.ignite.internal.visor.systemview.VisorSystemViewTask.SimpleType.STRING;

/** */
public class SystemViewCommand implements Command<SystemViewCommandArg, VisorSystemViewTaskResult> {
    /** Column separator. */
    public static final String COLUMN_SEPARATOR = "    ";

    /** {@inheritDoc} */
    @Override public String description() {
        return "Print system view content";
    }

    /** {@inheritDoc} */
    @Override public Class<SystemViewCommandArg> args() {
        return SystemViewCommandArg.class;
    }

    /** {@inheritDoc} */
    @Override public Class<VisorSystemViewTask> task() {
        return VisorSystemViewTask.class;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> nodes(Collection<UUID> nodes, SystemViewCommandArg arg) {
        if (arg.isAllNodes())
            return nodes;

        if (arg.getNodeIds() != null)
            return Arrays.asList(arg.getNodeIds());

        return arg.getNodeId() != null
                ? Collections.singleton(arg.getNodeId())
                : Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public void printResult(SystemViewCommandArg arg, VisorSystemViewTaskResult res, Consumer<String> printer) {
        if (res != null) {
            res.rows().forEach((nodeId, rows) -> {
                printer.accept("Results from node with ID: " + nodeId);
                printer.accept("---");

                printTable(res.attributes(), res.types(), rows, printer);

                printer.accept("---" + U.nl());
            });
        }
        else
            printer.accept("No system view with specified name was found [name=" + arg.getSystemViewName() + "]");
    }

    /**
     * Prints specified data rows as table.
     *
     * @param titles Titles of the table columns.
     * @param types  Types of the table columns.
     * @param data Table data rows.
     * @param printer Result printer.
     */
    public static void printTable(
        List<String> titles,
        List<VisorSystemViewTask.SimpleType> types,
        List<List<?>> data,
        Consumer<String> printer
    ) {
        List<Integer> colSzs;

        if (titles != null)
            colSzs = titles.stream().map(String::length).collect(Collectors.toList());
        else
            colSzs = types.stream().map(x -> 0).collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(data.size());

        data.forEach(row -> {
            ListIterator<Integer> colSzIter = colSzs.listIterator();

            rows.add(row.stream().map(val -> {
                String res = String.valueOf(val);

                colSzIter.set(Math.max(colSzIter.next(), res.length()));

                return res;
            }).collect(Collectors.toList()));
        });

        if (titles != null)
            printRow(titles, nCopies(titles.size(), STRING), colSzs, printer);

        rows.forEach(row -> printRow(row, types, colSzs, printer));
    }

    /**
     * Prints row content with respect to type and size of each column.
     *
     * @param row Row which content should be printed.
     * @param types Column types in sequential order for decent row formatting.
     * @param colSzs Column sizes in sequential order for decent row formatting.
     * @param printer Result printer.
     */
    private static void printRow(
        Collection<String> row,
        Collection<VisorSystemViewTask.SimpleType> types,
        Collection<Integer> colSzs,
        Consumer<String> printer
    ) {
        Iterator<VisorSystemViewTask.SimpleType> typeIter = types.iterator();
        Iterator<Integer> colSzsIter = colSzs.iterator();

        printer.accept(row.stream().map(colVal -> {
            VisorSystemViewTask.SimpleType colType = typeIter.next();

            int colSz = colSzsIter.next();

            String format = colType == DATE || colType == NUMBER ?
                "%" + colSz + "s" :
                "%-" + colSz + "s";

            return String.format(format, colVal);
        }).collect(Collectors.joining(COLUMN_SEPARATOR)));
    }
}
