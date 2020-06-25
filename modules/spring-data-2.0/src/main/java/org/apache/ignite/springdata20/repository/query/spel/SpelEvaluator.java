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

import java.util.Map;
import java.util.stream.Collectors;

import org.apache.ignite.springdata20.repository.query.spel.SpelQueryContext.SpelExtractor;
import org.springframework.data.repository.query.EvaluationContextProvider;
import org.springframework.data.repository.query.Parameters;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Evaluates SpEL expressions as extracted by the {@link SpelExtractor} based on parameter information from a method and
 * parameter values from a method call.
 *
 * @author Jens Schauder
 * @author Gerrit Meier
 * @author Oliver Gierke
 * @since spring data 2.1 - transition code (borrowed and adapted code from version 2.1)
 */
public class SpelEvaluator {

    private static final SpelExpressionParser PARSER = new SpelExpressionParser();
    private final EvaluationContextProvider evaluationContextProvider;
    private final Parameters<?, ?> parameters;
    private final SpelExtractor extractor;

    /**
     * @param evaluationContextProvider Evaluation context provider.
     * @param parameters Parameters.
     * @param extractor Extractor.
     */
    public SpelEvaluator(EvaluationContextProvider evaluationContextProvider,
        Parameters<?, ?> parameters,
        SpelExtractor extractor) {
        this.evaluationContextProvider = evaluationContextProvider;
        this.parameters = parameters;
        this.extractor = extractor;
    }

    /**
     * Evaluate all the SpEL expressions in {@link #parameters} based on values provided as an argument.
     *
     * @param values
     *     Parameter values. Must not be {@literal null}.
     * @return a map from parameter name to evaluated value. Guaranteed to be not {@literal null}.
     */
    public Map<String, Object> evaluate(Object[] values) {

        Assert.notNull(values, "Values must not be null.");

        EvaluationContext evaluationContext = evaluationContextProvider.getEvaluationContext(parameters, values);

        return extractor.getParameters().collect(Collectors.toMap(//
            it -> it.getKey(), //
            it -> getSpElValue(evaluationContext, it.getValue()) //
        ));
    }

    /**
     * Returns the query string produced by the intermediate SpEL expression collection step.
     *
     * @return the query string
     */
    public String getQueryString() {
        return extractor.getQueryString();
    }

    /**
     * @param evaluationContext Evaluation context.
     * @param expression Expression.
     */
    @Nullable
    private static Object getSpElValue(EvaluationContext evaluationContext, String expression) {
        return PARSER.parseExpression(expression).getValue(evaluationContext);
    }

}
