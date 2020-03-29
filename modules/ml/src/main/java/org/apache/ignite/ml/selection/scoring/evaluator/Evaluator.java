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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.dataset.impl.local.LocalDatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapData;
import org.apache.ignite.ml.dataset.primitive.FeatureMatrixWithLabelsOnHeapDataBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.KNNModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.selection.scoring.evaluator.aggregator.MetricStatsAggregator;
import org.apache.ignite.ml.selection.scoring.evaluator.context.EvaluationContext;
import org.apache.ignite.ml.selection.scoring.metric.Metric;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Evaluator that computes metrics from predictions and ground truth values.
 */
public class Evaluator {
    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(IgniteCache<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric
    ) {
        return evaluate(dataCache, (k, v) -> true, mdl, preprocessor, metric);
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric
    ) {

        return evaluate(
            new CacheBasedDatasetBuilder<>(Ignition.ignite(), dataCache, filter),
            mdl, preprocessor, metric
        ).getSingle();
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The metric name.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(IgniteCache<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        MetricName metric
    ) {

        return evaluate(dataCache, mdl, preprocessor, metric.create());
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The metric name.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        MetricName metric
    ) {

        return evaluate(
            new CacheBasedDatasetBuilder<>(Ignition.ignite(), dataCache, filter),
            mdl, preprocessor, metric.create()
        ).getSingle();
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given local data.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Map<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric
    ) {

        return evaluate(dataCache, (k, v) -> true, mdl, preprocessor, metric);
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Map<K, V> dataCache, IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric metric) {
        return evaluate(new LocalDatasetBuilder<>(dataCache, filter, 1), mdl, preprocessor, metric).getSingle();
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given local data.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Map<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        MetricName metric
    ) {

        return evaluate(dataCache, (k, v) -> true, mdl, preprocessor, metric.create());
    }

    /**
     * Computes the given metric on the given cache.
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param metric       The binary classification metric.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metric.
     */
    public static <K, V> double evaluate(Map<K, V> dataCache, IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        MetricName metric) {
        return evaluate(dataCache, filter, mdl, preprocessor, metric.create());
    }

    /**
     * Evaluate binary classifier by default metrics (see package classification). TODO: IGNITE-12156
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateBinaryClassification(IgniteCache<K, V> dataCache,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluateBinaryClassification(dataCache, (k, v) -> true, mdl, preprocessor);
    }

    /**
     * Evaluate binary classifier by default metrics (see package classification).
     *
     * @param dataCache    The given cache.
     * @param filter       The filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateBinaryClassification(IgniteCache<K, V> dataCache,
        // TODO: IGNITE-12156
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        Metric[] metrics = merge(
            MetricName.ACCURACY, MetricName.PRECISION, MetricName.RECALL, MetricName.F_MEASURE,
            MetricName.BALANCED_ACCURACY, MetricName.FALL_OUT, MetricName.FDR, MetricName.MISS_RATE,
            MetricName.NPV, MetricName.SPECIFICITY, MetricName.TRUE_POSITIVE, MetricName.FALSE_POSITIVE,
            MetricName.TRUE_NEGATIVE, MetricName.FALSE_NEGATIVE
        );
        return evaluate(new CacheBasedDatasetBuilder<>(Ignition.ignite(), dataCache, filter), mdl, preprocessor,
            metrics);
    }

    /**
     * Evaluate binary classifier by default metrics (see package classification).
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateBinaryClassification(Map<K, V> dataCache, // TODO: IGNITE-12156
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluateBinaryClassification(dataCache, (k, v) -> true, mdl, preprocessor);
    }

    /**
     * Evaluate binary classifier by default metrics (see package classification).
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateBinaryClassification(Map<K, V> dataCache, // TODO: IGNITE-12156
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluate(new LocalDatasetBuilder<>(dataCache, filter, 1), mdl, preprocessor,
            merge(MetricName.ACCURACY, MetricName.PRECISION, MetricName.RECALL, MetricName.F_MEASURE));
    }

    /**
     * Evaluate regression by default metrics (see package regression).
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateRegression(IgniteCache<K, V> dataCache, // TODO: IGNITE-12156
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluateRegression(dataCache, (k, v) -> true, mdl, preprocessor);
    }

    /**
     * Evaluate regression by default metrics (see package regression).
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateRegression(IgniteCache<K, V> dataCache, // TODO: IGNITE-12156
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluate(new CacheBasedDatasetBuilder<>(Ignition.ignite(), dataCache, filter), mdl, preprocessor,
            merge(MetricName.MAE, MetricName.MSE, MetricName.R2, MetricName.RMSE, MetricName.RSS));
    }

    /**
     * Evaluate regression by default metrics (see package regression).
     *
     * @param dataCache    The given cache.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateRegression(Map<K, V> dataCache, // TODO: IGNITE-12156
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluateRegression(dataCache, (k, v) -> true, mdl, preprocessor);
    }

    /**
     * Evaluate regression by default metrics (see package regression).
     *
     * @param dataCache    The given cache.
     * @param filter       The given filter.
     * @param mdl          The model.
     * @param preprocessor The preprocessor.
     * @param <K>          The type of cache entry key.
     * @param <V>          The type of cache entry value.
     * @return Computed metrics.
     */
    public static <K, V> EvaluationResult evaluateRegression(Map<K, V> dataCache, // TODO: IGNITE-12156
        IgniteBiPredicate<K, V> filter,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor) {

        return evaluate(new LocalDatasetBuilder<>(dataCache, filter, 1), mdl, preprocessor,
            merge(MetricName.MAE, MetricName.MSE, MetricName.R2, MetricName.RMSE, MetricName.RSS));
    }

    /**
     * Pack metric names to array of metrics.
     *
     * @param name1       Metric 1 name.
     * @param name2       Metric 2 name.
     * @param metricNames Metric Names.
     * @return Array of metrics.
     */
    private static Metric[] merge(MetricName name1, MetricName name2, MetricName... metricNames) {
        Metric[] metrics = new Metric[metricNames.length + 2];
        metrics[0] = name1.create();
        metrics[1] = name2.create();
        for (int i = 0; i < metricNames.length; i++)
            metrics[i + 2] = metricNames[i].create();
        return metrics;
    }

    /**
     * Evaluate model.
     *
     * @param mdl          The model.
     * @param dataCache    Dataset cache.
     * @param filter       Dataset filter.
     * @param preprocessor Preprocessor.
     * @param metrics      Metrics to compute.
     * @param <K>          Type of key.
     * @param <V>          Type of value.
     * @return Evaluation result.
     */
    private static <K, V> EvaluationResult evaluate(IgniteModel<Vector, Double> mdl,
        IgniteCache<K, V> dataCache,
        IgniteBiPredicate<K, V> filter,
        Preprocessor<K, V> preprocessor,
        Metric[] metrics) {
        return evaluate(new CacheBasedDatasetBuilder<>(Ignition.ignite(), dataCache, filter), mdl, preprocessor, metrics);
    }

    /**
     * Evaluate model.
     *
     * @param mdl            The model.
     * @param datasetBuilder Dataset builder.
     * @param preprocessor   Preprocessor.
     * @param metrics        Metrics to compute.
     * @param <K>            Type of key.
     * @param <V>            Type of value.
     * @return Evaluation result.
     */
    public static <K, V> EvaluationResult evaluate(DatasetBuilder<K, V> datasetBuilder,
        IgniteModel<Vector, Double> mdl,
        Preprocessor<K, V> preprocessor,
        Metric... metrics) {
        try (Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset = datasetBuilder.build(
            LearningEnvironmentBuilder.defaultBuilder(),
            new EmptyContextBuilder<>(),
            new FeatureMatrixWithLabelsOnHeapDataBuilder<>(preprocessor),
            LearningEnvironment.DEFAULT_TRAINER_ENV
        )) {
            IgniteCache<K, V> cache = null;
            if (datasetBuilder instanceof CacheBasedDatasetBuilder)
                cache = ((CacheBasedDatasetBuilder<K, V>)datasetBuilder).getUpstreamCache();

            return evaluate(mdl, dataset, cache, preprocessor, metrics);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Evaluate model.
     *
     * @param mdl          The model.
     * @param dataset      Dataset.
     * @param cache        Upstream cache.
     * @param preprocessor Preprocessor.
     * @param metrics      Metrics to compute.
     * @return Evaluation result.
     */
    @SuppressWarnings("unchecked")
    private static <K, V> EvaluationResult evaluate(IgniteModel<Vector, Double> mdl,
        Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset, IgniteCache<K, V> cache,
        Preprocessor<K, V> preprocessor, Metric[] metrics) {
        final Map<MetricName, Metric> metricMap = new HashMap<>();
        final Map<MetricName, Class> metricToAggrCls = new HashMap<>();
        for (Metric metric : metrics) {
            MetricStatsAggregator aggregator = metric.makeAggregator();
            MetricName name = metric.name();

            metricToAggrCls.put(name, aggregator.getClass());
            metricMap.put(name, metric);
        }

        Map<MetricName, Double> res = new HashMap<>();

        final Map<Class, EvaluationContext> aggrClsToCtx = initEvaluationContexts(dataset, metrics);
        final Map<Class, MetricStatsAggregator> aggrClsToAggr = computeStats(mdl, dataset, cache, preprocessor, aggrClsToCtx, metrics);

        for (Metric metric : metrics) {
            MetricName name = metric.name();
            Class aggrCls = metricToAggrCls.get(name);
            MetricStatsAggregator aggr = aggrClsToAggr.get(aggrCls);
            res.put(name, metricMap.get(name).initBy(aggr).value());
        }

        return new EvaluationResult(res);
    }

    /**
     * Inits evaluation contexts for metrics.
     *
     * @param dataset Dataset.
     * @param metrics Metrics.
     * @return Computed contexts.
     */
    @SuppressWarnings("unchecked")
    private static Map<Class, EvaluationContext> initEvaluationContexts(
        Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset,
        Metric... metrics
    ) {
        long nonEmptyCtxsCnt = Arrays.stream(metrics)
            .map(x -> x.makeAggregator().createInitializedContext())
            .filter(x -> ((EvaluationContext)x).needToCompute())
            .count();

        if (nonEmptyCtxsCnt == 0) {
            HashMap<Class, EvaluationContext> res = new HashMap<>();

            for (Metric m : metrics) {
                MetricStatsAggregator<Double, ?, ?> aggregator = m.makeAggregator();
                res.put(aggregator.getClass(), (EvaluationContext)m.makeAggregator().createInitializedContext());
                return res;
            }
        }

        return dataset.compute(data -> {
            Map<Class, MetricStatsAggregator> aggrs = new HashMap<>();
            for (Metric m : metrics) {
                MetricStatsAggregator<Double, ?, ?> aggregator = m.makeAggregator();
                if (!aggrs.containsKey(aggregator.getClass()))
                    aggrs.put(aggregator.getClass(), aggregator);
            }

            Map<Class, EvaluationContext> aggrToEvCtx = new HashMap<>();
            aggrs.forEach((clazz, aggr) -> aggrToEvCtx.put(clazz, (EvaluationContext)aggr.createInitializedContext()));

            for (int i = 0; i < data.getLabels().length; i++) {
                LabeledVector<Double> vector = VectorUtils.of(data.getFeatures()[i]).labeled(data.getLabels()[i]);
                aggrToEvCtx.values().forEach(ctx -> ctx.aggregate(vector));
            }
            return aggrToEvCtx;
        }, (left, right) -> {
            if (left == null && right == null)
                return new HashMap<>();

            if (left == null)
                return right;
            if (right == null)
                return left;

            HashMap<Class, EvaluationContext> res = new HashMap<>();
            for (Class key : left.keySet()) {
                EvaluationContext ctx1 = left.get(key);
                EvaluationContext ctx2 = right.get(key);
                A.ensure(ctx1 != null && ctx2 != null, "ctx1 != null && ctx2 != null");
                res.put(key, ctx1.mergeWith(ctx2));
            }
            return res;
        });
    }

    /**
     * Aggregates statistics for metrics evaluation.
     *
     * @param dataset Dataset.
     * @param metrics Metrics.
     * @return Aggregated statistics.
     */
    @SuppressWarnings("unchecked")
    private static <K, V> Map<Class, MetricStatsAggregator> computeStats(IgniteModel<Vector, Double> mdl,
        Dataset<EmptyContext, FeatureMatrixWithLabelsOnHeapData> dataset, IgniteCache<K, V> cache,
        Preprocessor<K, V> preprocessor,
        Map<Class, EvaluationContext> ctxs, Metric... metrics) {

        if (isOnlyLocalEstimation(mdl) && cache != null) {
            Map<Class, MetricStatsAggregator> aggrs = initAggregators(ctxs, metrics);

            try (QueryCursor<Cache.Entry<K, V>> qry = cache.query(new ScanQuery<>())) {
                qry.iterator().forEachRemaining(kv -> {
                    LabeledVector vector = preprocessor.apply(kv.getKey(), kv.getValue());

                    for (Class key : aggrs.keySet()) {
                        MetricStatsAggregator aggr = aggrs.get(key);
                        aggr.aggregate(mdl, vector);
                    }
                });
            }

            return aggrs;
        }
        else {
            return dataset.compute(data -> {
                Map<Class, MetricStatsAggregator> aggrs = initAggregators(ctxs, metrics);

                for (int i = 0; i < data.getLabels().length; i++) {
                    LabeledVector<Double> vector = VectorUtils.of(data.getFeatures()[i]).labeled(data.getLabels()[i]);
                    for (Class key : aggrs.keySet()) {
                        MetricStatsAggregator aggr = aggrs.get(key);
                        aggr.aggregate(mdl, vector);
                    }
                }

                return aggrs;
            }, (left, right) -> {
                if (left == null && right == null)
                    return new HashMap<>();
                if (left == null)
                    return right;
                if (right == null)
                    return left;

                HashMap<Class, MetricStatsAggregator> res = new HashMap<>();
                for (Class key : left.keySet()) {
                    MetricStatsAggregator agg1 = left.get(key);
                    MetricStatsAggregator agg2 = right.get(key);
                    A.ensure(agg1 != null && agg2 != null, "agg1 != null && agg2 != null");
                    res.put(key, agg1.mergeWith(agg2));
                }
                return res;
            });
        }
    }

    /**
     * Inits aggregators.
     *
     * @param ctxs    Evaluation contexts.
     * @param metrics Metrics.
     * @return Aggregators map.
     */
    private static Map<Class, MetricStatsAggregator> initAggregators(Map<Class, EvaluationContext> ctxs,
        Metric[] metrics) {
        Map<Class, MetricStatsAggregator> aggrs = new HashMap<>();
        for (Metric m : metrics) {
            MetricStatsAggregator aggregator = m.makeAggregator();
            EvaluationContext ctx = ctxs.get(aggregator.getClass());
            A.ensure(ctx != null, "ctx != null");
            aggregator.initByContext(ctx);

            if (!aggrs.containsKey(aggregator.getClass()))
                aggrs.put(aggregator.getClass(), aggregator);
        }
        return aggrs;
    }

    /**
     * Returns true if model should be evaluated only through scan query.
     *
     * @param mdl Model to estimation.
     * @return true if model should be evaluated only through scan query.
     */
    private static boolean isOnlyLocalEstimation(IgniteModel<Vector, Double> mdl) {
        return mdl instanceof KNNModel; // TODO:IGNITE-12156, mode this logic into model meta
    }
}
