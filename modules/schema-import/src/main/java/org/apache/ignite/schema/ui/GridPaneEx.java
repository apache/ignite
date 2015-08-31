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

package org.apache.ignite.schema.ui;

import javafx.geometry.Pos;
import javafx.scene.Node;
import javafx.scene.control.Label;
import javafx.scene.layout.ColumnConstraints;
import javafx.scene.layout.GridPane;
import javafx.scene.layout.Priority;
import javafx.scene.layout.RowConstraints;

/**
 * Utility extension of {@code GridPane}.
 */
public class GridPaneEx extends GridPane {
    /** Current column. */
    private int col;

    /** Current row. */
    private int row;

    /**
     * Create pane.
     */
    public GridPaneEx() {
        setAlignment(Pos.TOP_LEFT);
        setHgap(5);
        setVgap(10);
    }

    /**
     * Add default column.
     */
    public void addColumn() {
        getColumnConstraints().add(new ColumnConstraints());
    }

    /**
     * Add column with constraints and horizontal grow priority for the column.
     *
     * @param min Column minimum size.
     * @param pref Column preferred size.
     * @param max Column max size.
     * @param hgrow Column horizontal grow priority.
     */
    public void addColumn(double min, double pref, double max, Priority hgrow) {
        ColumnConstraints cc = new ColumnConstraints(min, pref, max);

        cc.setHgrow(hgrow);

        getColumnConstraints().add(cc);
    }

    /**
     * Add default row.
     */
    public void addRow() {
        getRowConstraints().add(new RowConstraints());
    }

    /**
     * Add default rows.
     *
     * @param n Number of rows to add.
     */
    public void addRows(int n) {
        for (int i = 0; i < n; i++)
            addRow();
    }

    /**
     * Add row with constraints and vertical grow priority for the row.
     *
     * @param min Row minimum size.
     * @param pref Row preferred size.
     * @param max Row max size.
     * @param vgrow Row vertical grow priority.
     */
    public void addRow(double min, double pref, double max, Priority vgrow) {
        RowConstraints rc = new RowConstraints(min, pref, max);

        rc.setVgrow(vgrow);

        getRowConstraints().add(rc);
    }

    /**
     * Wrap to next row.
     */
    public void wrap() {
        col = 0;

        row++;
    }

    /**
     * Skip columns.
     *
     * @param span How many columns should be skipped.
     */
    public void skip(int span) {
        add(new Label(""), span);
    }

    /**
     * Move to next column.
     */
    private void nextCol(int span) {
        col += span;

        if (col >= getColumnConstraints().size())
            wrap();
    }

    /**
     * Add control to grid pane.
     *
     * @param ctrl Control to add.
     * @param span How many columns control should take.
     * @return Added control.
     */
    public <T extends Node> T add(T ctrl, int span) {
        add(ctrl, col, row, span, 1);

        nextCol(span);

        return ctrl;
    }

    /**
     * Add control to grid pane.
     *
     * @param ctrl Control to add.
     * @return Added control.
     */
    public <T extends Node> T add(T ctrl) {
        return add(ctrl, 1);
    }

    /**
     * Add control with label.
     *
     * @param text Label text.
     * @param ctrl Control to add.
     * @param span How many columns control should take.
     * @return Added control.
     */
    public <T extends Node> T addLabeled(String text, T ctrl, int span) {
        add(new Label(text));

        return add(ctrl, span);
    }

    /**
     * Add control with label.
     *
     * @param text Label text.
     * @param ctrl Control to add.
     * @return Added control.
     */
    public <T extends Node> T addLabeled(String text, T ctrl) {
        return addLabeled(text, ctrl, 1);
    }
}