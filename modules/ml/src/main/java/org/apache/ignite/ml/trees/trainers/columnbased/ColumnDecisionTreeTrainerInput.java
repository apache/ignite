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

package org.apache.ignite.ml.trees.trainers.columnbased;

import org.apache.ignite.Ignite;
import org.apache.ignite.lang.IgniteBiTuple;

import java.util.Map;
import java.util.stream.Stream;

/**
 * Input for {@see ColumnDecisionTreeTrainer}.
 */
public interface ColumnDecisionTreeTrainerInput {
    /**
     * Projection of data on feature with the given index.
     *
     * @param idx Feature index.
     * @return Projection of data on feature with the given index.
     */
    Stream<IgniteBiTuple<Integer, Double>> values(int idx);

    /**
     * Labels.
     *
     * @param ignite Ignite instance.
     */
    double[] labels(Ignite ignite);

    /** Information about which features are categorical in the form of feature index -> number of categories. */
    Map<Integer, Integer> catFeaturesInfo();

    /** Number of features. */
    int featuresCount();

    /**
     * Get affinity key for the given column index.
     * Affinity key should be pure-functionally dependent from idx.
     */
    Object affinityKey(int idx, Ignite ignite);
}
