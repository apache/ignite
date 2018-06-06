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

package org.apache.ignite.ml.selection.cv;

import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.selection.score.ScoreCalculator;
import org.apache.ignite.ml.selection.score.TruthWithPrediction;
import org.apache.ignite.ml.selection.score.util.CacheBasedTruthWithPredictionCursor;
import org.apache.ignite.ml.selection.score.util.LocalTruthWithPredictionCursor;
import org.apache.ignite.ml.selection.split.mapper.UniformMapper;
import org.apache.ignite.ml.trainers.DatasetTrainer;

/**
 * Cross validation score calculator.
 *
 * @param <M> Type of model.
 * @param <L> Type of a label (truth or prediction).
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class CrossValidationScoreCalculator<M extends Model<double[], L>, L, K, V> {

    public double[] score(DatasetTrainer<M, L> trainer, Ignite ignite, IgniteCache<K, V> upstreamCache,
        IgniteBiPredicate<K, V> filter, IgniteBiFunction<K, V, double[]> featureExtractor,
        IgniteBiFunction<K, V, L> lbExtractor, ScoreCalculator<L> scoreCalculator, UniformMapper<K, V> mapper, int cv) {
        return score(
            trainer,
            predicate -> new CacheBasedDatasetBuilder<>(
                ignite,
                upstreamCache,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v)
            ),
            (predicate, mdl) -> new CacheBasedTruthWithPredictionCursor<>(
                upstreamCache,
                (k, v) -> filter.apply(k, v) && !predicate.apply(k, v),
                featureExtractor,
                lbExtractor,
                mdl
            ),
            featureExtractor,
            lbExtractor,
            scoreCalculator,
            mapper,
            cv
        );
    }

    public double[] score(DatasetTrainer<M, L> trainer, Map<K, V> upstreamMap, IgniteBiPredicate<K, V> filter,
        int parts, IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor,
        ScoreCalculator<L> scoreCalculator, UniformMapper<K, V> mapper, int cv) {
        return score(
            trainer,
            predicate -> new LocalDatasetBuilder<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && predicate.apply(k, v),
                parts
            ),
            (predicate, mdl) -> new LocalTruthWithPredictionCursor<>(
                upstreamMap,
                (k, v) -> filter.apply(k, v) && !predicate.apply(k, v),
                featureExtractor,
                lbExtractor,
                mdl
            ),
            featureExtractor,
            lbExtractor,
            scoreCalculator,
            mapper,
            cv
        );
    }

    private <R extends Iterable<TruthWithPrediction<L>> & AutoCloseable> double[] score(DatasetTrainer<M, L> trainer,
        Function<IgniteBiPredicate<K, V>, DatasetBuilder<K, V>> datasetBuilderSupplier,
        BiFunction<IgniteBiPredicate<K, V>, M, R> iterSupplier,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor,
        ScoreCalculator<L> scoreCalculator, UniformMapper<K, V> mapper, int cv) {

        double[] scores = new double[cv];

        double foldSize = 1.0 / cv;
        for (int i = 0; i < cv; i++) {
            double from = foldSize * i;
            double to = foldSize * (i + 1);
            IgniteBiPredicate<K, V> trainSetPred = (k, v) -> {
                double pnt = mapper.map(k, v);
                return pnt < from || pnt > to;
            };

            DatasetBuilder<K, V> datasetBuilder = datasetBuilderSupplier.apply(trainSetPred);
            M mdl = trainer.fit(datasetBuilder, featureExtractor, lbExtractor);

            try (R r = iterSupplier.apply(trainSetPred, mdl)) {
                scores[i] = scoreCalculator.score(r.iterator());
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return scores;

    }
}
