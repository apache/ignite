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

package org.apache.ignite.ml.tree.impurity;

import java.io.Serializable;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.impurity.util.StepFunction;

/**
 * Base interface for impurity measure calculators that calculates all impurity measures required to find a best split.
 *
 * @param <T> Type of impurity measure.
 */
public interface ImpurityMeasureCalculator<T extends ImpurityMeasure<T>> extends Serializable {
    /**
     * Calculates all impurity measures required required to find a best split and returns them as an array of
     * {@link StepFunction} (for every column).
     *
     * @param data Features and labels.
     * @return Impurity measures as an array of {@link StepFunction} (for every column).
     */
    public StepFunction<T>[] calculate(DecisionTreeData data);
}
