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

package org.springframework.data.ignite.repository.impl;

import java.lang.reflect.Method;
import org.jetbrains.annotations.NotNull;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 *
 */
public class QueryGenerator {
    /**
     * @param mtd Method.
     * @param metadata Metadata.
     */
    @NotNull public static String generateSql(Method mtd, RepositoryMetadata metadata) {
        PartTree parts = new PartTree(mtd.getName(), metadata.getDomainType());

        StringBuilder sql = new StringBuilder();

        if (parts.isDelete())
            sql.append("DELETE ");
        else {
            sql.append("SELECT ");

            if (parts.isDistinct())
                sql.append("DISTINCT ");

            sql.append(" * ");
        }

        sql.append("FROM ").append(metadata.getDomainType().getSimpleName());

        if (parts.iterator().hasNext()) {
            sql.append(" WHERE ");

            for (PartTree.OrPart orPart : parts) {
                sql.append("(");
                for (Part part : orPart) {
                    handlePart(sql, part);
                    sql.append(" AND ");
                }

                sql.delete(sql.length() - 5, sql.length());

                sql.append(") OR ");
            }

            sql.delete(sql.length() - 4, sql.length());
        }

        Sort sort = parts.getSort();

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

        if (parts.isLimiting()) {
            sql.append(" LIMIT ");
            sql.append(parts.getMaxResults().intValue());
        }

        return sql.toString();
    }

    private static void handlePart(StringBuilder sql, Part part) {
        sql.append("(");
        switch (part.getType()) {
            case SIMPLE_PROPERTY:
                sql.append(part.getProperty()).append("=?");
                break;
            case NEGATING_SIMPLE_PROPERTY:
                sql.append(part.getProperty()).append("!=?");
                break;
            case GREATER_THAN:
                sql.append(part.getProperty()).append(">?");
                break;
            case GREATER_THAN_EQUAL:
                sql.append(part.getProperty()).append(">=?");
                break;
            case LESS_THAN:
                sql.append(part.getProperty()).append("<?");
                break;
            case LESS_THAN_EQUAL:
                sql.append(part.getProperty()).append("<=?");
                break;
            case IS_NOT_NULL:
                sql.append(part.getProperty()).append(" IS NOT NULL");
                break;
            case IS_NULL:
                sql.append(part.getProperty()).append(" IS NULL");
                break;
            case BETWEEN:
                sql.append(part.getProperty()).append(" BETWEEN ? AND ?");
                break;
            case FALSE:
                sql.append(part.getProperty()).append(" = FALSE");
                break;
            case TRUE:
                sql.append(part.getProperty()).append(" = TRUE");
                break;
            case CONTAINING:
                sql.append(part.getProperty()).append(" LIKE '%' || ? || '%'");
                break;
            case NOT_CONTAINING:
                sql.append(part.getProperty()).append(" NOT LIKE '%' || ? || '%'");
                break;
            case LIKE:
                sql.append(part.getProperty()).append(" LIKE '%' || ? || '%'");
                break;
            case NOT_LIKE:
                sql.append(part.getProperty()).append(" NOT LIKE '%' || ? || '%'");
                break;
            case STARTING_WITH:
                sql.append(part.getProperty()).append(" LIKE  ? || '%'");
                break;
            case ENDING_WITH:
                sql.append(part.getProperty()).append(" LIKE '%' || ?");
                break;
            case IN:
                sql.append(part.getProperty()).append(" IN ?");
                break;
            case NOT_IN:
                sql.append(part.getProperty()).append(" NOT IN ?");
                break;
            case REGEX:
                sql.append(part.getProperty()).append(" REGEXP ?");
                break;
            case NEAR:
            case AFTER:
            case BEFORE:
            case EXISTS:
            default:
                throw new UnsupportedOperationException();
        }

        sql.append(")");
    }
}
