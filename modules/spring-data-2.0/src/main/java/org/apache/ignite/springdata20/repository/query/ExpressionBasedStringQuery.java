/*
 * Copyright 2013-2019 the original author or authors.
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

import java.util.regex.Pattern;
import org.springframework.data.repository.core.EntityMetadata;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.expression.Expression;
import org.springframework.expression.ParserContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.util.Assert;

/**
 * Extension of {@link StringQuery} that evaluates the given query string as a SpEL template-expression.
 * <p>
 * Currently the following template variables are available:
 * <ol>
 * <li>{@code #entityName} - the simple class name of the given entity</li>
 * <ol>
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Tom Hombergs
 */
class ExpressionBasedStringQuery extends StringQuery {
    /**
     * Expression parameter.
     */
    private static final String EXPRESSION_PARAMETER = "?#{";

    /**
     * Quoted expression parameter.
     */
    private static final String QUOTED_EXPRESSION_PARAMETER = "?__HASH__{";

    /**
     * Expression parameter quoting.
     */
    private static final Pattern EXPRESSION_PARAMETER_QUOTING = Pattern.compile(Pattern.quote(EXPRESSION_PARAMETER));

    /**
     * Expression parameter unquoting.
     */
    private static final Pattern EXPRESSION_PARAMETER_UNQUOTING = Pattern.compile(
        Pattern.quote(QUOTED_EXPRESSION_PARAMETER));

    /**
     * Entity name.
     */
    private static final String ENTITY_NAME = "entityName";

    /**
     * Entity name variable.
     */
    private static final String ENTITY_NAME_VARIABLE = "#" + ENTITY_NAME;

    /**
     * Entity name variable expression.
     */
    private static final String ENTITY_NAME_VARIABLE_EXPRESSION = "#{" + ENTITY_NAME_VARIABLE + "}";

    /**
     * Creates a new instance for the given query and {@link EntityMetadata}.
     *
     * @param qry      must not be {@literal null} or empty.
     * @param metadata must not be {@literal null}.
     * @param parser   must not be {@literal null}.
     */
    public ExpressionBasedStringQuery(String qry, RepositoryMetadata metadata, SpelExpressionParser parser) {
        super(renderQueryIfExpressionOrReturnQuery(qry, metadata, parser));
    }

    /**
     * Creates an instance from a given {@link DeclaredQuery}.
     *
     * @param qry      the original query. Must not be {@literal null}.
     * @param metadata the {@link RepositoryMetadata} for the given entity. Must not be {@literal null}.
     * @param parser   Parser for resolving SpEL expressions. Must not be {@literal null}.
     * @return A query supporting SpEL expressions.
     */
    static ExpressionBasedStringQuery from(DeclaredQuery qry,
        RepositoryMetadata metadata,
        SpelExpressionParser parser) {
        return new ExpressionBasedStringQuery(qry.getQueryString(), metadata, parser);
    }

    /**
     * @param qry,     the query expression potentially containing a SpEL expression. Must not be {@literal null}.}
     * @param metadata the {@link RepositoryMetadata} for the given entity. Must not be {@literal null}.
     * @param parser   Must not be {@literal null}.
     * @return rendered query
     */
    private static String renderQueryIfExpressionOrReturnQuery(String qry,
        RepositoryMetadata metadata,
        SpelExpressionParser parser) {

        Assert.notNull(qry, "query must not be null!");
        Assert.notNull(metadata, "metadata must not be null!");
        Assert.notNull(parser, "parser must not be null!");

        if (!containsExpression(qry))
            return qry;

        StandardEvaluationContext evalCtx = new StandardEvaluationContext();
        evalCtx.setVariable(ENTITY_NAME, metadata.getDomainType().getSimpleName());

        qry = potentiallyQuoteExpressionsParameter(qry);

        Expression expr = parser.parseExpression(qry, ParserContext.TEMPLATE_EXPRESSION);

        String result = expr.getValue(evalCtx, String.class);

        if (result == null)
            return qry;

        return potentiallyUnquoteParameterExpressions(result);
    }

    /**
     * @param result Result.
     */
    private static String potentiallyUnquoteParameterExpressions(String result) {
        return EXPRESSION_PARAMETER_UNQUOTING.matcher(result).replaceAll(EXPRESSION_PARAMETER);
    }

    /**
     * @param qry Query.
     */
    private static String potentiallyQuoteExpressionsParameter(String qry) {
        return EXPRESSION_PARAMETER_QUOTING.matcher(qry).replaceAll(QUOTED_EXPRESSION_PARAMETER);
    }

    /**
     * @param qry Query.
     */
    private static boolean containsExpression(String qry) {
        return qry.contains(ENTITY_NAME_VARIABLE_EXPRESSION);
    }
}
