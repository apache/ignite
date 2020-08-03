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

package org.apache.ignite.springdata.repository.query;

import java.lang.reflect.Method;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Ignite query generator for Spring Data framework.
 */
public class IgniteQueryGenerator {

    /**
     * @param mtd Method.
     * @param metadata Metadata.
     */
    @NotNull public static IgniteQuery generateSql(Method mtd, RepositoryMetadata metadata) {
        PartTree parts = new PartTree(mtd.getName(), metadata.getDomainType());

        StringBuilder sql = new StringBuilder();

        if (parts.isDelete())
            throw new UnsupportedOperationException("DELETE clause is not supported now.");
        else {
            sql.append("SELECT ");

            if (parts.isDistinct())
                throw new UnsupportedOperationException("DISTINCT clause in not supported.");

            if (parts.isCountProjection())
                sql.append("COUNT(1) ");
            else
                sql.append(" * ");
        }

        sql.append("FROM ").append(metadata.getDomainType().getSimpleName());

        if (parts.iterator().hasNext()) {
            sql.append(" WHERE ");

            for (PartTree.OrPart orPart : parts) {
                sql.append("(");
                for (Part part : orPart) {
                    handleQueryPart(sql, part);
                    sql.append(" AND ");
                }

                sql.delete(sql.length() - 5, sql.length());

                sql.append(") OR ");
            }

            sql.delete(sql.length() - 4, sql.length());
        }

        addSorting(sql, parts.getSort());

        if (parts.isLimiting()) {
            sql.append(" LIMIT ");
            sql.append(parts.getMaxResults().intValue());
        }

        return new IgniteQuery(sql.toString(), parts.isCountProjection(), getOptions(mtd));
    }

    /**
     * Add a dynamic part of query for the sorting support.
     *
     * @param sql SQL text string.
     * @param sort Sort method.
     */
    public static StringBuilder addSorting(StringBuilder sql, Sort sort) {
        if (sort != null) {
            sql.append(" ORDER BY ");

            for (Sort.Order order : sort) {
                sql.append(order.getProperty()).append(" ").append(order.getDirection());

                if (order.getNullHandling() != Sort.NullHandling.NATIVE) {
                    sql.append(" ").append("NULL ");
                    switch (order.getNullHandling()) {
                        case NULLS_FIRST:
                            sql.append("FIRST");
                            break;
                        case NULLS_LAST:
                            sql.append("LAST");
                            break;
                    }
                }
                sql.append(", ");
            }

            sql.delete(sql.length() - 2, sql.length());
        }

        return sql;
    }

    /**
     * Add a dynamic part of a query for the pagination support.
     *
     * @param sql Builder instance.
     * @param pageable Pageable instance.
     * @return Builder instance.
     */
    public static StringBuilder addPaging(StringBuilder sql, Pageable pageable) {
        if (pageable.getSort() != null)
            addSorting(sql, pageable.getSort());

        sql.append(" LIMIT ").append(pageable.getPageSize()).append(" OFFSET ").append(pageable.getOffset());

        return sql;
    }

    /**
     * Determines whether query is dynamic or not (by list of method parameters)
     *
     * @param mtd Method.
     * @return type of options
     */
    public static IgniteQuery.Option getOptions(Method mtd) {
        IgniteQuery.Option option = IgniteQuery.Option.NONE;

        Class<?>[] types = mtd.getParameterTypes();

        if (types.length > 0) {
            Class<?> type = types[types.length - 1];

            if (Sort.class.isAssignableFrom(type))
                option = IgniteQuery.Option.SORTING;
            else if (Pageable.class.isAssignableFrom(type))
                option = IgniteQuery.Option.PAGINATION;
        }

        for (int i = 0; i < types.length - 1; i++) {
            Class<?> tp = types[i];
            if (tp == Sort.class || tp == Pageable.class)
                throw new AssertionError("Sort and Pageable parameters are allowed only in the last position");
        }

        return option;
    }

    /**
     * Transform part to sql expression
     */
    private static void handleQueryPart(StringBuilder sql, Part part) {
        sql.append("(");

        sql.append(part.getProperty());

        switch (part.getType()) {
            case SIMPLE_PROPERTY:
                sql.append("=?");
                break;
            case NEGATING_SIMPLE_PROPERTY:
                sql.append("<>?");
                break;
            case GREATER_THAN:
                sql.append(">?");
                break;
            case GREATER_THAN_EQUAL:
                sql.append(">=?");
                break;
            case LESS_THAN:
                sql.append("<?");
                break;
            case LESS_THAN_EQUAL:
                sql.append("<=?");
                break;
            case IS_NOT_NULL:
                sql.append(" IS NOT NULL");
                break;
            case IS_NULL:
                sql.append(" IS NULL");
                break;
            case BETWEEN:
                sql.append(" BETWEEN ? AND ?");
                break;
            case FALSE:
                sql.append(" = FALSE");
                break;
            case TRUE:
                sql.append(" = TRUE");
                break;
            case CONTAINING:
                sql.append(" LIKE '%' || ? || '%'");
                break;
            case NOT_CONTAINING:
                sql.append(" NOT LIKE '%' || ? || '%'");
                break;
            case LIKE:
                sql.append(" LIKE '%' || ? || '%'");
                break;
            case NOT_LIKE:
                sql.append(" NOT LIKE '%' || ? || '%'");
                break;
            case STARTING_WITH:
                sql.append(" LIKE  ? || '%'");
                break;
            case ENDING_WITH:
                sql.append(" LIKE '%' || ?");
                break;
            case IN:
                sql.append(" IN ?");
                break;
            case NOT_IN:
                sql.append(" NOT IN ?");
                break;
            case REGEX:
                sql.append(" REGEXP ?");
                break;
            case NEAR:
            case AFTER:
            case BEFORE:
            case EXISTS:
            default:
                throw new UnsupportedOperationException(part.getType() + " is not supported!");
        }

        sql.append(")");
    }
}

