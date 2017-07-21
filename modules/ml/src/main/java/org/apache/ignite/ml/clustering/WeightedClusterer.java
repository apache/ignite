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

package org.apache.ignite.ml.clustering;

import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.exceptions.ConvergenceException;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;

/**
 * Support of clusterization with given weights.
 */
public interface WeightedClusterer<P, M extends Model> extends Clusterer<P, M> {
    /**
     * Perform clusterization of given points weighted by given weights.
     *
     * @param points Points.
     * @param k count of centers.
     * @param weights Weights.
     */
    public KMeansModel cluster(P points, int k, List<Double> weights) throws
        MathIllegalArgumentException, ConvergenceException;
}
