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

package org.apache.ignite.ml.selection.scoring.evaluator.aggregator;

import java.io.Serializable;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.context.EmptyContext;
import org.apache.ignite.ml.selection.scoring.evaluator.context.EvaluationContext;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Class represents aggregations for classification metric (including multiclassification case).
 *
 * @param <L> Type of label.
 */
public class ClassificationMetricsAggregator<L extends Serializable> implements MetricStatsAggregator<L, EmptyContext<L>, ClassificationMetricsAggregator<L>> {
    /**
     * Serial version uid.
     */
    private static final long serialVersionUID = 6508258921730584458L;

    /**
     * Valid answers count.
     */
    private long validAnswersCnt;

    /**
     * Total number of examples.
     */
    private long totalNumberOfExamples;

    /**
     * Creates an instance of ClassificationMetricsAggregator.
     */
    public ClassificationMetricsAggregator() {
    }

    /**
     * Creates an instance of ClassificationMetricsAggregator.
     *
     * @param validAnswersCount     Valid answers count.
     * @param totalNumberOfExamples Total number of examples.
     */
    public ClassificationMetricsAggregator(long validAnswersCount, long totalNumberOfExamples) {
        this.validAnswersCnt = validAnswersCount;
        this.totalNumberOfExamples = totalNumberOfExamples;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void aggregate(IgniteModel<Vector, L> model, LabeledVector<L> vector) {
        L modelAns = model.predict(vector.features());
        L truth = vector.label();
        if (modelAns.equals(truth))
            validAnswersCnt++;
        totalNumberOfExamples++;
    }

    /**
     * {@inheritDoc}
     */
    @Override public ClassificationMetricsAggregator<L> mergeWith(ClassificationMetricsAggregator<L> other) {
        return new ClassificationMetricsAggregator<>(
            this.validAnswersCnt + other.validAnswersCnt,
            this.totalNumberOfExamples + other.totalNumberOfExamples
        );
    }

    /**
     * {@inheritDoc}
     */
    @Override public EmptyContext<L> createUnitializedContext() {
        return EvaluationContext.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override public void initByContext(EmptyContext<L> context) {

    }

    /**
     * Returns validAnswersCount.
     *
     * @return validAnswersCount.
     */
    public long getValidAnswersCount() {
        return validAnswersCnt;
    }

    /**
     * Returns totalNumberOfExamples.
     *
     * @return totalNumberOfExamples.
     */
    public long getTotalNumberOfExamples() {
        return totalNumberOfExamples;
    }
}
