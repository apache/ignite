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
import java.util.Collection;
import picocli.CommandLine.Help.Ansi.Text;
import picocli.CommandLine.Help.ColorScheme;

public class Table {
    private final int indent;

    private final ColorScheme colorScheme;

    private final Collection<Text[]> data = new ArrayList<>();

    private int[] lengths;

    public Table(int indent, ColorScheme colorScheme) {
        if (indent < 0)
            throw new IllegalArgumentException("Indent can't be negative.");

        this.indent = indent;
        this.colorScheme = colorScheme;
    }

    public void addRow(Object... items) {
        if (lengths == null) {
            lengths = new int[items.length];
        }
        else if (items.length != lengths.length) {
            throw new IllegalArgumentException("Wrong number of items.");
        }

        Text[] row = new Text[items.length];

        for (int i = 0; i < items.length; i++) {
            Text item = colorScheme.text(items[i].toString());

            row[i] = item;

            lengths[i] = Math.max(lengths[i], item.getCJKAdjustedLength());
        }

        data.add(row);
    }

    @Override public String toString() {
        String indentStr = " ".repeat(indent);

        StringBuilder sb = new StringBuilder();

        for (Text[] row : data) {
            sb.append(indentStr);

            appendLine(sb);
            appendRow(sb, row);
        }

        appendLine(sb);

        return sb.toString();
    }

    private void appendLine(StringBuilder sb) {
        for (int length : lengths) {
            sb.append('+').append("-".repeat(length + 2));
        }

        sb.append("+\n");
    }

    private void appendRow(StringBuilder sb, Text[] row) {
        assert row.length == lengths.length;

        for (int i = 0; i < row.length; i++) {
            Text item = row[i];

            sb.append("| ").append(item.toString()).append(" ".repeat(lengths[i] + 1 - item.getCJKAdjustedLength()));
        }

        sb.append("|\n");
    }
}
