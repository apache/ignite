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

package org.apache.ignite.internal.processors.query.h2.sql;

import java.util.Objects;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Table alias in SQL statement contains alias name and reference to real table.
 */
// TODO VO: May be we can remove it completely.
public class TableAlias {
    /** Alias. */
    private final String alias;

    /** Table. */
    private final GridH2Table tbl;

    /**
     * @param alias Alias.
     * @param tbl Table
     */
    public TableAlias(String alias, GridH2Table tbl) {
        this.alias = alias;
        this.tbl = tbl;
    }

    /**
     * @return table alias.
     */
    public String alias() {
        return alias;
    }

    /**
     * @return H2 table.
     */
    public GridH2Table table() {
        return tbl;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        return F.eq(alias, ((TableAlias)o).alias);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        // TODO VO: Rempve tbl.
        // TODO VO: Do not use Objects.hash
        return Objects.hash(alias, tbl);
    }
}