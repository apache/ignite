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

import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.locks.Condition;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQueryParser;
import org.apache.ignite.internal.util.typedef.F;
import org.h2.command.dml.Select;
import org.h2.expression.Aggregate;
import org.h2.expression.Alias;
import org.h2.expression.CompareLike;
import org.h2.expression.Comparison;
import org.h2.expression.ConditionAndOr;
import org.h2.expression.ConditionExists;
import org.h2.expression.ConditionIn;
import org.h2.expression.ConditionInConstantSet;
import org.h2.expression.ConditionInParameter;
import org.h2.expression.ConditionInSelect;
import org.h2.expression.ConditionNot;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.ExpressionList;
import org.h2.expression.Function;
import org.h2.expression.JavaAggregate;
import org.h2.expression.JavaFunction;
import org.h2.expression.Operation;
import org.h2.expression.SequenceValue;
import org.h2.expression.Subquery;
import org.h2.expression.TableFunction;
import org.h2.expression.Wildcard;
import org.h2.result.SortOrder;
import org.h2.table.TableFilter;

/**
 *
 */
public class GridH2UsedColumnInfo {
    /** Columns to extract from full row. */
    private final Set<Integer> colsToExtract;

    /** Columns count in full row. */
    private final int colsCnt;

    /**
     * @param colsToExtract Columns to extract from full row.
     * @param colsCnt Columns count in full row.
     */
    public GridH2UsedColumnInfo(Set<Integer> colsToExtract, int colsCnt) {
        this.colsToExtract = colsToExtract;
        this.colsCnt = colsCnt;
    }

   /**
     * @return Columns IDs to extract from full row.
     */
    public Set<Integer> columns() {
        return colsToExtract;
    }

    /**
     * @return Columns count in full row.
     */
    public int columnsCount() {
        return colsCnt;
    }

    /**
     * @param f Table filter.
     * @return Information about columns are used in query execution.
     */
    public static GridH2UsedColumnInfo extractUsedColumns(TableFilter f) {
        Set<Integer> colsToExtract = new HashSet<Integer>(f.getTable().getColumns().length);

        GridSqlQueryParser parser = new GridSqlQueryParser(false);

        parser.extractUsedColumnsFromQuery(f, colsToExtract, f.getSelect());

        if (F.isEmpty(colsToExtract) || colsToExtract.size() == f.getTable().getColumns().length)
            return null;
        else {
            System.out.println("+++ " + f.getTable().getName() + ":" + f.getTable().getColumns().length + " " + colsToExtract);
            return new GridH2UsedColumnInfo(colsToExtract, f.getTable().getColumns().length);
        }
    }
}