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

package org.apache.ignite.internal.processors.query.h2.opt;

import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.h2.command.Prepared;
import org.h2.command.dml.Query;
import org.h2.command.dml.Select;
import org.h2.command.dml.SelectUnion;
import org.h2.table.TableFilter;

/**
 * The purpose of this class is the cleaning of H2's {@link PreparedStatement} after the query has been executed.
 * The actual cleaning is carried out in {@code close()} method.
 */
public class GridH2StatementCleaner implements AutoCloseable {
    /** Filters to be cleaned after the query is done. */
    private final List<TableFilter> filters;

    /**
     * Private constructor.
     */
    private GridH2StatementCleaner(List<TableFilter> filters) {
        this.filters = filters;
    }

    /**
     * Fabric method. Creates Statement cleaner for the given statement.
     * @param stmt Statement.
     * @return Statement cleaner.
     */
    public static GridH2StatementCleaner fromPrepared(PreparedStatement stmt) {
        Prepared p = GridSqlQueryParser.prepared(stmt);

        if (!(p instanceof Query))
            return null;

        List<TableFilter> filters = new ArrayList<>();

        collectTableFilters((Query)p, filters);

        return new GridH2StatementCleaner(filters);
    }

    /**
     * Gathers all {@link TableFilter} into the dedicated collection.
     *
     * @param qry Query.
     * @param filters Filters.
     */
    private static void collectTableFilters(Query qry, List<TableFilter> filters) {
        if (qry.isUnion()) {
            SelectUnion union = (SelectUnion)qry;

            collectTableFilters(union.getLeft(), filters);
            collectTableFilters(union.getRight(), filters);
        }
        else {
            Select select = (Select)qry;
            select.getTopTableFilter().visit(new TableFilter.TableFilterVisitor() {
                @Override public void accept(TableFilter f) {
                    filters.add(f);
                }
            });
        }
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        for (TableFilter f : filters)
            f.set(null); // Help GC to collect current rows.
    }
}
