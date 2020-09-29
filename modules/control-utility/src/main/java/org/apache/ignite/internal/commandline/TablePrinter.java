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

package org.apache.ignite.internal.commandline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.ignite.util.SimpleType;

import static java.util.Collections.nCopies;
import static org.apache.ignite.util.SimpleType.DATE;
import static org.apache.ignite.util.SimpleType.NUMBER;
import static org.apache.ignite.util.SimpleType.STRING;

/** Represents utility class for table content printing. */
public class TablePrinter {
    /** Column separator. */
    public static final String COLUMN_SEPARATOR = "    ";

    /** Titles of the table columns. */
    private final List<String> titles;

    /** Types of the table columns. */
    private final List<SimpleType> types;

    /** Number of table columns. */
    private final int colsCnt;

    /**
     * @param titles Titles of the table columns. Number of titles determines the number of columns in table.
     * @param types  Types of the table columns.
     */
    public TablePrinter(List<String> titles, List<SimpleType> types) {
        this.titles = titles;
        colsCnt = titles.size();

        if (colsCnt != types.size())
            throw new IllegalArgumentException("The number of types must be equal to the number of columns.");

        this.types = types;
    }

    /** @param titles Titles of the table columns. Number of titles determines the number of columns in table. */
    public TablePrinter(List<String> titles) {
        this.titles = titles;
        colsCnt = titles.size();
        types = nCopies(colsCnt, STRING);
    }

    /**
     * Prints table content.
     *
     * @param data Table data rows.
     * @param log Logger.
     */
    public void print(List<List<?>> data, Logger log) {
        List<Integer> colSzs = titles.stream().map(String::length).collect(Collectors.toList());

        List<List<String>> rows = new ArrayList<>(data.size());

        data.forEach(row -> {
            if (row.size() != colsCnt) {
                throw new IllegalArgumentException(
                    "Number of the elements for each data row must be equal to the number of columns.");
            }

            ListIterator<Integer> colSzIter = colSzs.listIterator();

            rows.add(row.stream().map(val -> {
                String res = formatValue(val);

                colSzIter.set(Math.max(colSzIter.next(), res.length()));

                return res;
            }).collect(Collectors.toList()));
        });

        printRow(titles, nCopies(titles.size(), STRING), colSzs, log);

        rows.forEach(row -> printRow(row, types, colSzs, log));
    }

    /**
     * Prints row content with respect to type and size of each column.
     *
     * @param row Row which content should be printed.
     * @param types Column types in sequential order for decent row formatting.
     * @param colSzs Column sizes in sequential order for decent row formatting.
     * @param log Logger.
     */
    private void printRow(
        Collection<String> row,
        Collection<SimpleType> types,
        Collection<Integer> colSzs,
        Logger log
    ) {
        Iterator<SimpleType> typeIter = types.iterator();
        Iterator<Integer> colSzIter = colSzs.iterator();

        log.info(row.stream()
            .map(val -> formatColumn(typeIter.next(), colSzIter.next(), val))
            .collect(Collectors.joining(COLUMN_SEPARATOR)));
    }

    /**
     * Formats the specified value.
     *
     * @param val Value to format.
     * @return Formatted value.
     */
    protected String formatValue(Object val) {
        return String.valueOf(val);
    }

    /**
     * Formats given value to be printed as a part of the table column.
     *
     * @param type Type of the column.
     * @param colSz Size of the column.
     * @param val Value to format.
     * @return Formatted table column value which is ready for printing.
     */
    protected String formatColumn(SimpleType type, int colSz, String val) {
        String format = type == DATE || type == NUMBER ?
            "%" + colSz + "s" :
            "%-" + colSz + "s";

        return String.format(format, val);
    }
}
