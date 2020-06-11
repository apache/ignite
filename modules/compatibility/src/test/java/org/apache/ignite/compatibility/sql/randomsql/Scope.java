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
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.apache.ignite.compatibility.sql.randomsql.ast.ColumnRef;
import org.apache.ignite.compatibility.sql.randomsql.ast.TableRef;

import static org.apache.ignite.compatibility.sql.randomsql.ast.AstUtils.pickRandom;

/**
 * Scope: visible tables and columns.
 */
public class Scope {
    /** */
    private final Scope parentScope;

    /** Table available for column references. */
    private final List<TableRef> scopeTbls;

    /** */
    private Schema schema;

    /** */
    private Random rnd;

    /**
     * Constructor for child scope.
     *
     * @param parent Parent.
     */
    public Scope(Scope parent) {
        parentScope = parent;
        schema = parent.schema;
        rnd = parent.rnd;
        scopeTbls = new ArrayList<>();
    }

    /**
     * Constructor for root scope.
     *
     * @param schema Schema.
     * @param seed Random seed.
     */
    public Scope(Schema schema, int seed) {
        parentScope = null;
        this.schema = schema;
        rnd = new Random(seed);
        scopeTbls = new ArrayList<>();
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
    public Schema schema() {
        return schema;
    }

    public Table pickRandomTable() {
        return pickRandom(schema.tables(), rnd);
    }

    public TableRef pickRandomTableRef() {
        return pickRandom(scopeTbls, rnd);
    }

    public ColumnRef pickRandomColumn(Class<?> type) {
        if (type == null) {
            TableRef tbl = pickRandomTableRef();
            return pickRandom(tbl.columnRefs(), rnd);
        }
        else {
            List<ColumnRef> cols = new ArrayList<>();
            for (TableRef tbl : scopeTbls) {
                for (ColumnRef col : tbl.columnRefs()) {
                    if (col.typeClass() == type)
                        cols.add(col);
                }
            }
            return pickRandom(cols, rnd);
        }
    }

    public Operator pickRandomOp(Class<?> left, Class<?> right, Class<?> resultType) {
        List<Operator> matchedOps = schema.operators().stream()
            .filter(op -> op.left() == left && op.right() == right && op.result() == resultType )
            .collect(Collectors.toList());

        return pickRandom(matchedOps, rnd);
    }

    public void addScopeTable(TableRef t) {
        scopeTbls.add(t);
    }

    public Random random() {
        return rnd;
    }
}
