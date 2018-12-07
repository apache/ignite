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

package org.apache.ignite.ml.selection.scoring.evaluator;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.math3.util.Pair;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.cursor.CacheBasedLabelPairCursor;
import org.apache.ignite.ml.selection.scoring.cursor.LabelPairCursor;
import org.apache.ignite.ml.selection.scoring.metric.Metric;

/**
 * Binary classification evaluator that computes metrics from predictions and ground truth values.
 */
public class Evaluator {
    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache The given cache.
     * @param mdl The model.
     * @param featureExtractor The feature extractor.
     * @param lbExtractor The label extractor.
     * @param metric The binary classification metric.
     * @param <L> The type of label.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> double evaluate(IgniteCache<K, V> dataCache,
        Model<Vector, L> mdl,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor,
        Metric<L> metric) {

        return calculateMetric(dataCache, null, mdl, featureExtractor, lbExtractor, metric);
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache The given cache.
     * @param filter The given filter.
     * @param mdl The model.
     * @param featureExtractor The feature extractor.
     * @param lbExtractor The label extractor.
     * @param metric The binary classification metric.
     * @param <L> The type of label.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> double evaluate(IgniteCache<K, V> dataCache,  IgniteBiPredicate<K, V> filter,
        Model<Vector, L> mdl,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor,
        Metric<L> metric) {

        return calculateMetric(dataCache, filter, mdl, featureExtractor, lbExtractor, metric);
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache The given cache.
     * @param mdl The model.
     * @param featureExtractor The feature extractor.
     * @param lbExtractor The label extractor.
     * @param metrics The list of binary classification metrics.
     * @param <L> The type of label.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> Map<String, Double> evaluate(IgniteCache<K, V> dataCache,
        Model<Vector, L> mdl,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor,
        List<Metric<L>> metrics) {

        return calculateMultipleMetrics(dataCache, null, mdl, featureExtractor, lbExtractor, metrics);
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache The given cache.
     * @param filter The given filter.
     * @param mdl The model.
     * @param featureExtractor The feature extractor.
     * @param lbExtractor The label extractor.
     * @param metrics The list of binary classification metrics.
     * @param <L> The type of label.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    public static <L, K, V> Map<String, Double> evaluate(IgniteCache<K, V> dataCache, IgniteBiPredicate<K, V> filter,
        Model<Vector, L> mdl,
        IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor,
        List<Metric<L>> metrics) {

        return calculateMultipleMetrics(dataCache, filter, mdl, featureExtractor, lbExtractor, metrics);
    }

    /**
     * Computes the given metrics on the given cache.
     *
     * @param dataCache The given cache.
     * @param filter The given filter.
     * @param mdl The model.
     * @param featureExtractor The feature extractor.
     * @param lbExtractor The label extractor.
     * @param metrics The list of binary classification metrics.
     * @param <L> The type of label.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    private static <L, K, V> Map<String, Double> calculateMultipleMetrics(IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter, Model<Vector, L> mdl, IgniteBiFunction<K, V, Vector> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor, List<Metric<L>> metrics) {
        return metrics.stream().map(metric -> {
            double score = calculateMetric(dataCache, filter, mdl, featureExtractor, lbExtractor, metric);
            return new Pair<>(metric.name(), score);
        }).collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (p1, p2) -> p1));
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache The given cache.
     * @param filter The given filter.
     * @param mdl The model.
     * @param featureExtractor The feature extractor.
     * @param lbExtractor The label extractor.
     * @param metric The binary classification metric.
     * @param <L> The type of label.
     * @param <K> The type of cache entry key.
     * @param <V> The type of cache entry value.
     * @return Computed metric.
     */
    private static <L, K, V> double calculateMetric(IgniteCache<K, V> dataCache, IgniteBiPredicate<K, V> filter,
        Model<Vector, L> mdl, IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor,
        Metric<L> metric) {
        double metricRes;

        try (LabelPairCursor<L> cursor = new CacheBasedLabelPairCursor<>(
            dataCache,
            filter,
            featureExtractor,
            lbExtractor,
            mdl
        )) {
            metricRes = metric.score(cursor.iterator());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        return metricRes;
    }
}
