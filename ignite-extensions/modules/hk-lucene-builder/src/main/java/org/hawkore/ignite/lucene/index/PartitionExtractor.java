package org.hawkore.ignite.lucene.index;

import java.util.HashSet;
import java.util.Set;

import org.apache.ignite.IgniteException;

import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.h2.dml.DmlAstUtils;
import org.apache.ignite.internal.processors.query.h2.opt.GridH2Table;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlAst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlColumn;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlConst;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlElement;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperation;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlOperationType;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlParameter;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlQuery;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlSelect;
import org.apache.ignite.internal.processors.query.h2.sql.GridSqlTable;
import org.apache.ignite.lang.IgniteBiTuple;
import org.h2.table.Column;

public class PartitionExtractor {

	

    /**
     * Extract lucene query.
     *
     * @param qry
     *     the qry
     * @param params
     *     the params
     * @return the ignite bi tuple
     */
    public static IgniteBiTuple<GridSqlColumn, Search> extractLuceneQuery(GridSqlQuery qry, Object[] params){

        if (!(qry instanceof GridSqlSelect))
            return new IgniteBiTuple<>(null, null);

        GridSqlSelect select = (GridSqlSelect)qry;

        if (select.from() == null)
            return new IgniteBiTuple<>(null, null);

        return extractLuceneQueryFromConditions(select, select.where(), params);
    }

    private static IgniteBiTuple<GridSqlColumn, Search> extractLuceneQueryFromConditions(GridSqlSelect select, GridSqlAst el, Object[] params) {

        if (!(el instanceof GridSqlOperation))
            return new IgniteBiTuple<>(null, null);

        GridSqlOperation op = (GridSqlOperation)el;

        switch (op.operationType()) {

            case IN:
            case EQUAL: {

                IgniteBiTuple<GridSqlColumn, Search> condition = extractLuceneConditionFromEquality(select, op, params);

                if (condition == null){
                    return new IgniteBiTuple<>(null, null);
                }

                return condition;
            }

            case AND: {
                assert op.size() == 2;

                IgniteBiTuple<GridSqlColumn, Search> conditionLeft = extractLuceneQueryFromConditions(select, op.child(0), params);
                IgniteBiTuple<GridSqlColumn, Search> conditionRight = extractLuceneQueryFromConditions(select, op.child(1), params);

                if (conditionLeft != null && conditionRight != null && conditionLeft.getKey() != null && conditionRight.getKey() != null){
                    // kind of conflict on advanced JSON condition (lucene = '{...}') and (lucene = '{...}')
                    throw new IgniteException("Only one lucene filter condition is allowed on query filter");
                }

                if (conditionLeft != null && conditionLeft.getKey() != null)
                    return conditionLeft;

                if (conditionRight != null && conditionRight.getKey() != null)
                    return conditionRight;

                return new IgniteBiTuple<>(null, null);
            }

            default:
                return new IgniteBiTuple<>(null, null);
        }
    }


    /** return IgniteBiTuple<GridSqlColumn, Search> === lucene query column, advanced lucene JSON search */
    public static IgniteBiTuple<GridSqlColumn, Search> extractLuceneConditionFromEquality(GridSqlSelect select,
        GridSqlAst el,
        Object[] params) {

        GridSqlOperation op = (GridSqlOperation)el;

        if (op.operationType() != GridSqlOperationType.EQUAL && op.operationType() != GridSqlOperationType.IN) {
            return null;
        }

        GridSqlElement left = op.child(0);

        if (!(left instanceof GridSqlColumn)) {
            return null;
        }

        GridSqlColumn column = (GridSqlColumn)left;

        // find main query table on select statement
        Set<GridSqlTable> tbls = new HashSet<>();

        DmlAstUtils.collectAllGridTablesInTarget((GridSqlElement)select.from(), tbls);

        if (tbls.isEmpty()) {
            return null;
        }

        // first table
        GridSqlTable tab = tbls.iterator().next();
        GridH2Table table = tab.dataTable();

        // internal H2 tables
        if (table == null) {
            return null;
        }

        Column luceneColumn = null;
        try {
            luceneColumn = table.getColumn(QueryUtils.LUCENE_FIELD_NAME);
        } catch (Exception e) {
            //table has not a lucene column
            return null;
        }

        // check if it is a lucene column
        if (!column.column().equals(luceneColumn)) {
            return null;
        }

        // lucene sort on reduce query will be required only if table is partitioned
        if (!table.isPartitioned()) {
            return new IgniteBiTuple<>(column, null);
        }


        String luceneQuery = null;

        // search lucene query
        // Add support to IN values on right element. See GridSqlQueryParser.parseExpression0
        for (int i = 1; i < op.size(); i++) {

            GridSqlElement right = op.child(i);

            // check assign
            if (!(right instanceof GridSqlConst) && !(right instanceof GridSqlParameter)) {
                return null;
            }

            if (right instanceof GridSqlConst) {
                GridSqlConst constant = (GridSqlConst)right;
                luceneQuery = constant.value().getString();
            } else {
                GridSqlParameter param = (GridSqlParameter)right;
                luceneQuery = params[param.index()].toString();
            }
            // found
            if (luceneQuery != null && !luceneQuery.isEmpty()) {
                break;
            }
        }

        if (luceneQuery == null || luceneQuery.isEmpty()) {
            return new IgniteBiTuple<>(column, null);
        }
        Search search = null;
        try {
            search = SearchBuilder.fromJson(luceneQuery).build();
        } catch (Exception e) {
            //just not an Advanced lucene JSON search
        }
        return new IgniteBiTuple<>(column, search);
    }
	
}
