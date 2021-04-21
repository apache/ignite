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
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Classes with such interface are responsible for metric statistics aggregations used for metric computations over
 * given dataset.
 *
 * @param <L>    Label class.
 * @param <Ctx>  Context class.
 * @param <Self> Aggregator class.
 */
public interface MetricStatsAggregator<L, Ctx, Self extends MetricStatsAggregator<L, Ctx, ? super Self>> extends Serializable {
    /**
     * Aggregates statistics for metric computation given model and vector with answer.
     *
     * @param model  Model.
     * @param vector Vector.
     */
    public void aggregate(IgniteModel<Vector, L> model, LabeledVector<L> vector);

    /**
     * Merges statistics of two aggregators to new aggreagator.
     *
     * @param other Other aggregator.
     * @return New aggregator.
     */
    public Self mergeWith(Self other);

    /**
     * Returns initialized context.
     *
     * @return initialized evaluation context.
     */
    public Ctx createInitializedContext();

    /**
     * Inits this aggtegator by evaluation context.
     *
     * @param ctx Evaluation context.
     */
    public void initByContext(Ctx ctx);
}
