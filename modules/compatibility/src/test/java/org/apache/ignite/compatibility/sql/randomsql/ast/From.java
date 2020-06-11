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
package org.apache.ignite.compatibility.sql.randomsql.ast;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.compatibility.sql.randomsql.Table;

/**
 * TODO: Add class description.
 */
public class From extends Ast {
    private final List<TableRef> from = new ArrayList<>();

    public From(Ast parent) {
        super(parent);

        do {
            Table t = scope.pickRandomTable();

            from.add(new TableRef(this, t));

            scope.addScopeTable(t);
        } while (rnd.nextInt(100) > 20);
    }

    @Override public void print(StringBuilder out) {
        for (int i = 0; i < from.size(); i++) {
            if (i != 0)
                out.append(",");

            out.append(" ");
            from.get(i).print(out);
        }
    }
}
