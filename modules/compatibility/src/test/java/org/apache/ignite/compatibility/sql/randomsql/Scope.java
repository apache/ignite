/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql.randomsql;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import static org.apache.ignite.compatibility.sql.randomsql.ast.AstUtils.pickRandom;

/**
 * Scope: visible tables and columns.
 */
public class Scope {
    /** */
    private final Scope parentScope;

    /** All available tables. */
    private final List<Table> allTbls;

    /** Table available for column references. */
    private final List<Table> scopeTbls;

    /** */
    private Schema schema;

    /** */
    private Random rnd;

    /**
     * @param parent Parent.
     */
    public Scope(Scope parent) {
        parentScope = parent;
        allTbls = parent == null ? new ArrayList<>() : parent.allTbls;
        schema = parent == null ? null : parent.schema;
        rnd = parent == null ? null : parent.rnd; // TODO refactor
        scopeTbls = new ArrayList<>();
    }

    /** */
    public void fillTables(Collection<Table> tbls) {
        this.allTbls.addAll(tbls);
    }

    /** */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    /** */
    public void setRandom(Random rnd) {
        this.rnd = rnd;
    }

    /** */
    public Scope parentScope() {
        return parentScope;
    }

    /** */
    public List<Table> allTables() {
        return allTbls;
    }

    public Table pickRandomTable() {
        return pickRandom(allTbls, rnd);
    }

    public Column pickRandomColumn(Class<?> type) {
        if (type == null) {
            Table tbl = pickRandomTable();
            return pickRandom(tbl.columnsList(), rnd);
        }
        else {
            List<Column> cols = new ArrayList<>();
            for (Table tbl : scopeTbls) {
                for (Column col : tbl.columnsList()) {
                    if (col.typeClass() == type)
                        cols.add(col);
                }
            }
            return pickRandom(cols, rnd);
        }
    }

    public void addScopeTable(Table t) {
        scopeTbls.add(t);
    }
}
