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

package org.apache.ignite.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import picocli.CommandLine.Help.Ansi.Text;
import picocli.CommandLine.Help.ColorScheme;

/**
 * Basic implementation of an ascii table. Supports styling via {@link ColorScheme}.
 */
public class Table {
    /** Indent size. */
    private final int indent;

    /** Color scheme for table output. */
    private final ColorScheme cs;

    /** Table data */
    private final Collection<Row> data = new ArrayList<>();

    /** Columns lengths. */
    private int[] lengths;

    /**
     * Creates a new table.
     *
     * @param indent Left-side indentation (i.e. the provided number of spaces will be added to every line in the
     *               output).
     * @param cs     Color scheme.
     */
    public Table(int indent, ColorScheme cs) {
        if (indent < 0)
            throw new IllegalArgumentException("Indent can't be negative.");

        this.indent = indent;
        this.cs = cs;
    }

    /**
     * Adds a row.
     *
     * @param items List of items in the row. Every item is converted to a string and styled based on the provided
     *              {@link ColorScheme}. If an instance of {@link Text} is provided, it is added as-is.
     */
    public void addRow(Object... items) {
        if (lengths == null)
            lengths = new int[items.length];
        else if (items.length != lengths.length)
            throw new IllegalArgumentException("Wrong number of items.");

        Text[] row = new Text[items.length];

        for (int i = 0; i < items.length; i++) {
            Object item = items[i];

            Text text = item instanceof Text ? (Text)item : cs.text(item.toString());

            row[i] = text;

            lengths[i] = Math.max(lengths[i], text.getCJKAdjustedLength());
        }

        data.add(new DataRow(row));
    }

    /**
     * Adds a section. Title spans all columns in the table.
     *
     * @param title Section title.
     */
    public void addSection(Object title) {
        Text text = title instanceof Text ? (Text)title : cs.text(title.toString());

        data.add(new SectionTitle(text));
    }

    /**
     * Converts the table to a string.
     *
     * @return String representation of this table.
     */
    @Override public String toString() {
        if (data.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();

        for (Row row : data) {
            appendLine(sb);
            appendRow(sb, row);
        }

        appendLine(sb);

        return sb.toString().stripTrailing();
    }

    /** */
    private void appendLine(StringBuilder sb) {
        sb.append(" ".repeat(indent));

        for (int length : lengths) {
            sb.append('+').append("-".repeat(length + 2));
        }

        sb.append("+\n");
    }

    /** */
    private void appendRow(StringBuilder sb, Row row) {
        sb.append(" ".repeat(indent))
            .append(row.render())
            .append('\n');
    }

    /** Interface for any renderable row. */
    private interface Row {
        /** Render row to string. */
        String render();
    }

    /**
     * Row with actual data.
     */
    private class DataRow implements Row {
        /** */
        private final Text[] row;

        /** */
        DataRow(Text[] row) {
            this.row = row;
        }

        /** {@inheritDoc} */
        @Override public String render() {
            assert row.length == lengths.length;

            StringBuilder sb = new StringBuilder();

            for (int i = 0; i < row.length; i++) {
                Text item = row[i];

                sb.append("| ")
                    .append(item.toString())
                    .append(" ".repeat(lengths[i] + 1 - item.getCJKAdjustedLength()));
            }

            sb.append("|");

            return sb.toString();
        }
    }

    /**
     * Row with table title.
     */
    private class SectionTitle implements Row {
        /** */
        private final Text title;

        /** */
        SectionTitle(Text title) {
            this.title = title;
        }

        /** {@inheritDoc} */
        @Override public String render() {
            int totalLen = Arrays.stream(lengths).sum() + 3 * (lengths.length - 1);

            return "| " + title.toString() + " ".repeat(totalLen - title.getCJKAdjustedLength()) + " |";
        }
    }
}
