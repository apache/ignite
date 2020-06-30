/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.springdata20.repository.query;

import java.util.List;
import org.jetbrains.annotations.Nullable;
import org.springframework.util.StringUtils;

/**
 * A wrapper for a String representation of a query offering information about the query.
 *
 * @author Jens Schauder
 */
interface DeclaredQuery {
    /**
     * Creates a {@literal DeclaredQuery} from a query {@literal String}.
     *
     * @param qry might be {@literal null} or empty.
     * @return a {@literal DeclaredQuery} instance even for a {@literal null} or empty argument.
     */
    public static DeclaredQuery of(@Nullable String qry) {
        return StringUtils.isEmpty(qry) ? EmptyDeclaredQuery.EMPTY_QUERY : new StringQuery(qry);
    }

    /**
     * @return whether the underlying query has at least one named parameter.
     */
    public boolean hasNamedParameter();

    /**
     * Returns the query string.
     */
    public String getQueryString();

    /**
     * Returns the main alias used in the query.
     *
     * @return the alias
     */
    @Nullable
    public String getAlias();

    /**
     * Returns whether the query is using a constructor expression.
     */
    public boolean hasConstructorExpression();

    /**
     * Returns whether the query uses the default projection, i.e. returns the main alias defined for the query.
     */
    public boolean isDefaultProjection();

    /**
     * Returns the {@link StringQuery.ParameterBinding}s registered.
     */
    public List<StringQuery.ParameterBinding> getParameterBindings();

    /**
     * Creates a new {@literal DeclaredQuery} representing a count query, i.e. a query returning the number of rows to
     * be expected from the original query, either derived from the query wrapped by this instance or from the
     * information passed as arguments.
     *
     * @param cntQry           an optional query string to be used if present.
     * @param cntQryProjection an optional return type for the query.
     * @return a new {@literal DeclaredQuery} instance.
     */
    public DeclaredQuery deriveCountQuery(@Nullable String cntQry, @Nullable String cntQryProjection);

    /**
     * @return whether paging is implemented in the query itself, e.g. using SpEL expressions.
     */
    public default boolean usesPaging() {
        return false;
    }

    /**
     * Returns whether the query uses JDBC style parameters, i.e. parameters denoted by a simple ? without any index or
     * name.
     *
     * @return Whether the query uses JDBC style parameters.
     */
    public boolean usesJdbcStyleParameters();
}
