/*
 * Copyright 2008-2019 the original author or authors.
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

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static java.util.regex.Pattern.DOTALL;
import static java.util.regex.Pattern.compile;

/**
 * Simple utility class to create queries.
 *
 * @author Oliver Gierke
 * @author Kevin Raymond
 * @author Thomas Darimont
 * @author Komi Innocent
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Sébastien Péralta
 * @author Jens Schauder
 * @author Nils Borrmann
 * @author Reda.Housni -Alaoui
 */
public abstract class QueryUtils {
    /**
     * The constant COUNT_QUERY_STRING.
     */
    public static final String COUNT_QUERY_STRING = "select count(%s) from %s x";

    /**
     * The constant DELETE_ALL_QUERY_STRING.
     */
    public static final String DELETE_ALL_QUERY_STRING = "delete from %s x";

    /**
     * Used Regex/Unicode categories (see http://www.unicode.org/reports/tr18/#General_Category_Property): Z Separator
     * Cc Control Cf Format P Punctuation
     */
    private static final String IDENTIFIER = "[._[\\P{Z}&&\\P{Cc}&&\\P{Cf}&&\\P{P}]]+";

    /**
     * The Colon no double colon.
     */
    static final String COLON_NO_DOUBLE_COLON = "(?<![:\\\\]):";

    /**
     * The Identifier group.
     */
    static final String IDENTIFIER_GROUP = String.format("(%s)", IDENTIFIER);

    /** */
    private static final String COUNT_REPLACEMENT_TEMPLATE = "select count(%s) $5$6$7";

    /** */
    private static final String SIMPLE_COUNT_VALUE = "$2";

    /** */
    private static final String COMPLEX_COUNT_VALUE = "$3$6";

    /** */
    private static final String ORDER_BY_PART = "(?iu)\\s+order\\s+by\\s+.*$";

    /** */
    private static final Pattern ALIAS_MATCH;

    /** */
    private static final Pattern COUNT_MATCH;

    /** */
    private static final Pattern PROJECTION_CLAUSE = Pattern
        .compile("select\\s+(.+)\\s+from", Pattern.CASE_INSENSITIVE);

    /** */
    private static final String JOIN = "join\\s+(fetch\\s+)?" + IDENTIFIER + "\\s+(as\\s+)?" + IDENTIFIER_GROUP;

    /** */
    private static final Pattern JOIN_PATTERN = Pattern.compile(JOIN, Pattern.CASE_INSENSITIVE);

    /** */
    private static final String EQUALS_CONDITION_STRING = "%s.%s = :%s";

    /** */
    private static final Pattern NAMED_PARAMETER = Pattern.compile(
        COLON_NO_DOUBLE_COLON + IDENTIFIER + "|\\#" + IDENTIFIER, CASE_INSENSITIVE);

    /** */
    private static final Pattern CONSTRUCTOR_EXPRESSION;

    /** */
    private static final int QUERY_JOIN_ALIAS_GROUP_INDEX = 3;

    /** */
    private static final int VARIABLE_NAME_GROUP_INDEX = 4;

    /** */
    private static final Pattern FUNCTION_PATTERN;

    static {
        StringBuilder builder = new StringBuilder();
        builder.append("(?<=from)"); // from as starting delimiter
        builder.append("(?:\\s)+"); // at least one space separating
        builder.append(IDENTIFIER_GROUP); // Entity name, can be qualified (any
        builder.append("(?:\\sas)*"); // exclude possible "as" keyword
        builder.append("(?:\\s)+"); // at least one space separating
        builder.append("(?!(?:where))(\\w+)"); // the actual alias

        ALIAS_MATCH = compile(builder.toString(), CASE_INSENSITIVE);

        builder = new StringBuilder();
        builder.append("(select\\s+((distinct )?(.+?)?)\\s+)?(from\\s+");
        builder.append(IDENTIFIER);
        builder.append("(?:\\s+as)?\\s+)");
        builder.append(IDENTIFIER_GROUP);
        builder.append("(.*)");

        COUNT_MATCH = compile(builder.toString(), CASE_INSENSITIVE);

        builder = new StringBuilder();
        builder.append("select");
        builder.append("\\s+"); // at least one space separating
        builder.append("(.*\\s+)?"); // anything in between (e.g. distinct) at least one space separating
        builder.append("new");
        builder.append("\\s+"); // at least one space separating
        builder.append(IDENTIFIER);
        builder.append("\\s*"); // zero to unlimited space separating
        builder.append("\\(");
        builder.append(".*");
        builder.append("\\)");

        CONSTRUCTOR_EXPRESSION = compile(builder.toString(), CASE_INSENSITIVE + DOTALL);

        builder = new StringBuilder();
        // any function call including parameters within the brackets
        builder.append("\\w+\\s*\\([\\w\\.,\\s'=]+\\)");
        // the potential alias
        builder.append("\\s+[as|AS]+\\s+(([\\w\\.]+))");

        FUNCTION_PATTERN = compile(builder.toString());
    }

    /**
     * Private constructor to prevent instantiation.
     */
    private QueryUtils() {
        // No-op.
    }

    /**
     * Returns the query string to execute an exists query for the given id attributes.
     *
     * @param entityName        the name of the entity to create the query for, must not be {@literal null}.
     * @param cntQryPlaceHolder the placeholder for the count clause, must not be {@literal null}.
     * @param idAttrs           the id attributes for the entity, must not be {@literal null}.
     * @return the exists query string
     */
    public static String getExistsQueryString(String entityName,
        String cntQryPlaceHolder,
        Iterable<String> idAttrs) {
        String whereClause = Streamable.of(idAttrs).stream() //
            .map(idAttribute -> String.format(EQUALS_CONDITION_STRING, "x", idAttribute,
                idAttribute)) //
            .collect(Collectors.joining(" AND ", " WHERE ", ""));

        return String.format(COUNT_QUERY_STRING, cntQryPlaceHolder, entityName) + whereClause;
    }

    /**
     * Returns the query string for the given class name.
     *
     * @param template   must not be {@literal null}.
     * @param entityName must not be {@literal null}.
     * @return the template with placeholders replaced by the {@literal entityName}. Guaranteed to be not {@literal
     *     null}.
     */
    public static String getQueryString(String template, String entityName) {
        Assert.hasText(entityName, "Entity name must not be null or empty!");

        return String.format(template, entityName);
    }

    /**
     * Returns the aliases used for {@code left (outer) join}s.
     *
     * @param qry a query string to extract the aliases of joins from. Must not be {@literal null}.
     * @return a {@literal Set} of aliases used in the query. Guaranteed to be not {@literal null}.
     */
    static Set<String> getOuterJoinAliases(String qry) {
        Set<String> result = new HashSet<>();
        Matcher matcher = JOIN_PATTERN.matcher(qry);

        while (matcher.find()) {
            String alias = matcher.group(QUERY_JOIN_ALIAS_GROUP_INDEX);
            if (StringUtils.hasText(alias))
                result.add(alias);
        }

        return result;
    }

    /**
     * Returns the aliases used for aggregate functions like {@code SUM, COUNT, ...}.
     *
     * @param qry a {@literal String} containing a query. Must not be {@literal null}.
     * @return a {@literal Set} containing all found aliases. Guaranteed to be not {@literal null}.
     */
    static Set<String> getFunctionAliases(String qry) {
        Set<String> result = new HashSet<>();
        Matcher matcher = FUNCTION_PATTERN.matcher(qry);

        while (matcher.find()) {
            String alias = matcher.group(1);

            if (StringUtils.hasText(alias))
                result.add(alias);
        }

        return result;
    }

    /**
     * Resolves the alias for the entity to be retrieved from the given JPA query.
     *
     * @param qry must not be {@literal null}.
     * @return Might return {@literal null}.
     */
    @Nullable
    static String detectAlias(String qry) {
        Matcher matcher = ALIAS_MATCH.matcher(qry);

        return matcher.find() ? matcher.group(2) : null;
    }

    /**
     * Creates a count projected query from the given original query.
     *
     * @param originalQry   must not be {@literal null}.
     * @param cntProjection may be {@literal null}.
     * @return a query String to be used a count query for pagination. Guaranteed to be not {@literal null}.
     */
    static String createCountQueryFor(String originalQry, @Nullable String cntProjection) {
        Assert.hasText(originalQry, "OriginalQuery must not be null or empty!");

        Matcher matcher = COUNT_MATCH.matcher(originalQry);
        String countQuery;

        if (cntProjection == null) {
            String variable = matcher.matches() ? matcher.group(VARIABLE_NAME_GROUP_INDEX) : null;
            boolean useVariable = variable != null && StringUtils.hasText(variable) && !variable.startsWith("new")
                && !variable.startsWith("count(") && !variable.contains(",");

            String replacement = useVariable ? SIMPLE_COUNT_VALUE : COMPLEX_COUNT_VALUE;
            countQuery = matcher.replaceFirst(String.format(COUNT_REPLACEMENT_TEMPLATE, replacement));
        }
        else
            countQuery = matcher.replaceFirst(String.format(COUNT_REPLACEMENT_TEMPLATE, cntProjection));

        return countQuery.replaceFirst(ORDER_BY_PART, "");
    }

    /**
     * Returns whether the given JPQL query contains a constructor expression.
     *
     * @param qry must not be {@literal null} or empty.
     * @return boolean
     */
    public static boolean hasConstructorExpression(String qry) {
        Assert.hasText(qry, "Query must not be null or empty!");

        return CONSTRUCTOR_EXPRESSION.matcher(qry).find();
    }

    /**
     * Returns the projection part of the query, i.e. everything between {@code select} and {@code from}.
     *
     * @param qry must not be {@literal null} or empty.
     * @return projection
     */
    public static String getProjection(String qry) {
        Assert.hasText(qry, "Query must not be null or empty!");

        Matcher matcher = PROJECTION_CLAUSE.matcher(qry);
        String projection = matcher.find() ? matcher.group(1) : "";
        return projection.trim();
    }
}
