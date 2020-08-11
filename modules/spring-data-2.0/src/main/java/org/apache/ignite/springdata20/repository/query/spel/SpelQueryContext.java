/*
 * Copyright 2018-2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.springdata20.repository.query.spel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.jetbrains.annotations.Nullable;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Range.Bound;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.Parameters;
import org.springframework.util.Assert;

/**
 * Source of {@link SpelExtractor} encapsulating configuration often common for all queries.
 *
 * @author Jens Schauder
 * @author Gerrit Meier
 */
public class SpelQueryContext {
    /** */
    private static final String SPEL_PATTERN_STRING = "([:?])#\\{([^}]+)}";

    /** */
    private static final Pattern SPEL_PATTERN = Pattern.compile(SPEL_PATTERN_STRING);

    /**
     * A function from the index of a SpEL expression in a query and the actual SpEL expression to the parameter name to
     * be used in place of the SpEL expression. A typical implementation is expected to look like
     * <code>(index, spel) -> "__some_placeholder_" + index</code>
     */
    private final BiFunction<Integer, String, String> paramNameSrc;

    /**
     * A function from a prefix used to demarcate a SpEL expression in a query and a parameter name as returned from
     * {@link #paramNameSrc} to a {@literal String} to be used as a replacement of the SpEL in the query. The returned
     * value should normally be interpretable as a bind parameter by the underlying persistence mechanism. A typical
     * implementation is expected to look like <code>(prefix, name) -> prefix + name</code> or
     * <code>(prefix, name) -> "{" + name + "}"</code>
     */
    private final BiFunction<String, String, String> replacementSrc;

    /** */
    private SpelQueryContext(BiFunction<Integer, String, String> paramNameSrc,
        BiFunction<String, String, String> replacementSrc) {
        this.paramNameSrc = paramNameSrc;
        this.replacementSrc = replacementSrc;
    }

    /**
     * Of spel query context.
     *
     * @param parameterNameSource the parameter name source
     * @param replacementSource   the replacement source
     * @return the spel query context
     */
    public static SpelQueryContext of(BiFunction<Integer, String, String> parameterNameSource,
        BiFunction<String, String, String> replacementSource) {
        return new SpelQueryContext(parameterNameSource, replacementSource);
    }

    /**
     * Parses the query for SpEL expressions using the pattern:
     *
     * <pre>
     * &lt;prefix&gt;#{&lt;spel&gt;}
     * </pre>
     * <p>
     * with prefix being the character ':' or '?'. Parsing honors quoted {@literal String}s enclosed in single or double
     * quotation marks.
     *
     * @param qry a query containing SpEL expressions in the format described above. Must not be {@literal null}.
     * @return A {@link SpelExtractor} which makes the query with SpEL expressions replaced by bind parameters and a map
     * from bind parameter to SpEL expression available. Guaranteed to be not {@literal null}.
     */
    public SpelExtractor parse(String qry) {
        return new SpelExtractor(qry);
    }

    /**
     * Createsa {@link EvaluatingSpelQueryContext} from the current one and the given {@link
     * EvaluationContextProvider}*.
     *
     * @param provider must not be {@literal null}.
     * @return Evaluating Spel QueryContext
     */
    public EvaluatingSpelQueryContext withEvaluationContextProvider(EvaluationContextProvider provider) {
        Assert.notNull(provider, "EvaluationContextProvider must not be null!");

        return new EvaluatingSpelQueryContext(provider, paramNameSrc, replacementSrc);
    }

    /**
     * An extension of {@link SpelQueryContext} that can create {@link SpelEvaluator} instances as it also knows about a
     * {@link EvaluationContextProvider}.
     *
     * @author Oliver Gierke
     */
    public static class EvaluatingSpelQueryContext extends SpelQueryContext {
        /** */
        private final EvaluationContextProvider evaluationContextProvider;

        /**
         * Creates a new {@link EvaluatingSpelQueryContext} for the given {@link EvaluationContextProvider}, parameter
         * name source and replacement source.
         *
         * @param evaluationCtxProvider must not be {@literal null}.
         * @param paramNameSrc          must not be {@literal null}.
         * @param replacementSrc        must not be {@literal null}.
         */
        private EvaluatingSpelQueryContext(EvaluationContextProvider evaluationCtxProvider,
            BiFunction<Integer, String, String> paramNameSrc, BiFunction<String, String, String> replacementSrc) {
            super(paramNameSrc, replacementSrc);

            evaluationContextProvider = evaluationCtxProvider;
        }

        /**
         * Parses the query for SpEL expressions using the pattern:
         *
         * <pre>
         * &lt;prefix&gt;#{&lt;spel&gt;}
         * </pre>
         * <p>
         * with prefix being the character ':' or '?'. Parsing honors quoted {@literal String}s enclosed in single or
         * double quotation marks.
         *
         * @param qry        a query containing SpEL expressions in the format described above. Must not be {@literal
         *                   null}.
         * @param parameters a {@link Parameters} instance describing query method parameters
         * @return A {@link SpelEvaluator} which allows to evaluate the SpEL expressions. Will never be {@literal null}.
         */
        public SpelEvaluator parse(String qry, Parameters<?, ?> parameters) {
            return new SpelEvaluator(evaluationContextProvider, parameters, parse(qry));
        }
    }

    /**
     * Parses a query string, identifies the contained SpEL expressions, replaces them with bind parameters and offers a
     * {@link Map} from those bind parameters to the spel expression.
     * <p>
     * The parser detects quoted parts of the query string and does not detect SpEL expressions inside such quoted parts
     * of the query.
     *
     * @author Jens Schauder
     * @author Oliver Gierke
     */
    public class SpelExtractor {
        /** */
        private static final int PREFIX_GROUP_INDEX = 1;

        /** */
        private static final int EXPRESSION_GROUP_INDEX = 2;

        /** */
        private final String query;

        /** */
        private final Map<String, String> expressions;

        /** */
        private final QuotationMap quotations;

        /**
         * Creates a SpelExtractor from a query String.
         *
         * @param qry must not be {@literal null}.
         */
        SpelExtractor(String qry) {
            Assert.notNull(qry, "Query must not be null");

            Map<String, String> exps = new HashMap<>();
            Matcher matcher = SPEL_PATTERN.matcher(qry);
            StringBuilder resultQry = new StringBuilder();
            QuotationMap quotedAreas = new QuotationMap(qry);

            int expressionCounter = 0;
            int matchedUntil = 0;

            while (matcher.find()) {
                if (quotedAreas.isQuoted(matcher.start()))
                    resultQry.append(qry, matchedUntil, matcher.end());

                else {
                    String spelExpression = matcher.group(EXPRESSION_GROUP_INDEX);
                    String prefix = matcher.group(PREFIX_GROUP_INDEX);

                    String paramName = paramNameSrc.apply(expressionCounter, spelExpression);
                    String replacement = replacementSrc.apply(prefix, paramName);

                    resultQry.append(qry, matchedUntil, matcher.start());
                    resultQry.append(replacement);

                    exps.put(paramName, spelExpression);
                    expressionCounter++;
                }

                matchedUntil = matcher.end();
            }

            resultQry.append(qry.substring(matchedUntil));

            expressions = Collections.unmodifiableMap(exps);
            query = resultQry.toString();
            quotations = quotedAreas;
        }

        /**
         * The query with all the SpEL expressions replaced with bind parameters.
         *
         * @return Guaranteed to be not {@literal null}.
         */
        public String getQueryString() {
            return query;
        }

        /**
         * Is quoted.
         *
         * @param idx the idx
         * @return the boolean
         */
        public boolean isQuoted(int idx) {
            return quotations.isQuoted(idx);
        }

        /**
         * Gets parameter.
         *
         * @param name the name
         * @return the parameter
         */
        public String getParameter(String name) {
            return expressions.get(name);
        }

        /**
         * A {@literal Map} from parameter name to SpEL expression.
         *
         * @return Guaranteed to be not {@literal null}.
         */
        Map<String, String> getParameterMap() {
            return expressions;
        }

        /**
         * Gets parameters.
         *
         * @return the parameters
         */
        Stream<Entry<String, String>> getParameters() {
            return expressions.entrySet().stream();
        }
    }

    /**
     * Value object to analyze a {@link String} to determine the parts of the {@link String} that are quoted and offers
     * an API to query that information.
     *
     * @author Jens Schauder
     * @author Oliver Gierke
     */
    static class QuotationMap {
        /** */
        private static final Collection<Character> QUOTING_CHARACTERS = Arrays.asList('"', '\'');

        /** */
        private final List<Range<Integer>> quotedRanges = new ArrayList<>();

        /**
         * Creates a new {@link QuotationMap} for the query.
         *
         * @param qry can be {@literal null}.
         */
        public QuotationMap(@Nullable String qry) {
            if (qry == null)
                return;

            Character inQuotation = null;
            int start = 0;

            for (int i = 0; i < qry.length(); i++) {

                char curChar = qry.charAt(i);

                if (QUOTING_CHARACTERS.contains(curChar)) {

                    if (inQuotation == null) {

                        inQuotation = curChar;
                        start = i;

                    }
                    else if (curChar == inQuotation) {

                        inQuotation = null;

                        quotedRanges.add(Range.from(Bound.inclusive(start)).to(Bound.inclusive(i)));
                    }
                }
            }

            if (inQuotation != null) {
                throw new IllegalArgumentException(
                    String.format("The string <%s> starts a quoted range at %d, but never ends it.", qry, start));
            }
        }

        /**
         * Checks if a given index is within a quoted range.
         *
         * @param idx to check if it is part of a quoted range.
         * @return whether the query contains a quoted range at {@literal index}.
         */
        public boolean isQuoted(int idx) {
            return quotedRanges.stream().anyMatch(r -> r.contains(idx));
        }
    }
}
