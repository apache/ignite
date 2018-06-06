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

package org.apache.ignite.ml.selection.score.util;

import java.util.Iterator;
import java.util.Map;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.selection.score.TruthWithPrediction;
import org.jetbrains.annotations.NotNull;

public class LocalTruthWithPredictionCursor<L, K, V> implements TruthWithPredictionCursor<L> {

    private final Map<K, V> upstreamMap;

    private final IgniteBiPredicate<K, V> filter;

    private final IgniteBiFunction<K, V, double[]> featureExtractor;

    private final IgniteBiFunction<K, V, L> lbExtractor;

    private final Model<double[], L> mdl;

    public LocalTruthWithPredictionCursor(Map<K, V> upstreamMap, IgniteBiPredicate<K, V> filter,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor,
        Model<double[], L> mdl) {
        this.upstreamMap = upstreamMap;
        this.filter = filter;
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.mdl = mdl;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        /* Do nothing. */
    }

    /** {@inheritDoc} */
    @NotNull @Override public Iterator<TruthWithPrediction<L>> iterator() {
        return null;
    }
}
