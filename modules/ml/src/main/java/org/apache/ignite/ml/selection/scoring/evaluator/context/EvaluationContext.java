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

package org.apache.ignite.ml.selection.scoring.evaluator.context;

import java.io.Serializable;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Classes with this interface are responsible for preparatory computations before model evaluation. For example if we
 * don't know what is positive and negative label we can define it in automatically way using such evaluation context.
 *
 * @param <L>    Type of label.
 * @param <Self> Type of evaluation context.
 */
public interface EvaluationContext<L, Self extends EvaluationContext<L, ? super Self>> extends Serializable {
    /**
     * Aggregates statistic from vector of sample.
     *
     * @param vector Vector.
     */
    public void aggregate(LabeledVector<L> vector);

    /**
     * Merges statistics of this and other context.
     *
     * @param other Other context.
     * @return New merged context.
     */
    public Self mergeWith(Self other);

    /**
     * Returns true if this contexts should be evaluated through map-reduce.
     *
     * @return True if this contexts should be evaluated through map-reduce.
     */
    public default boolean needToCompute() {
        return true;
    }

    /**
     * Returns default empty context.
     *
     * @return Empty context.
     */
    public static <L extends Serializable> EmptyContext<L> empty() {
        return new EmptyContext<>();
    }
}
