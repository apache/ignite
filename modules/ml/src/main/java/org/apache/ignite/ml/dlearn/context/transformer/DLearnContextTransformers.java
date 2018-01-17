package org.apache.ignite.ml.dlearn.context.transformer;

import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnContextFactory;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnContextFactory;
import org.apache.ignite.ml.dlearn.context.transformer.cache.CacheDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.cache.CacheLabeledDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.local.LocalDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.local.LocalLabeledDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.dataset.DLearnDataset;
import org.apache.ignite.ml.dlearn.dataset.DLearnLabeledDataset;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/**
 * Aggregator which allows to find desired transformer from one learning context into another. This class doesn't
 * introduce a new functionality, but helps to work efficiently with existing transformers.
 */
public class DLearnContextTransformers {
    /**
     * Creates a transformer which accepts cache learning context (produced by {@link CacheDLearnContextFactory}) and
     * constructs {@link DLearnDataset}.
     *
     * @param featureExtractor feature extractor
     * @param <K> type of keys in cache learning context
     * @param <V> type of values in cache learning context
     * @return transformer
     */
    public static <K, V> CacheDatasetDLearnPartitionTransformer<K, V> cacheToDataset(
        IgniteBiFunction<K, V, double[]> featureExtractor) {
        return new CacheDatasetDLearnPartitionTransformer<>(featureExtractor);
    }

    /**
     * Creates a transformer which accepts cache learning context (produced by {@link CacheDLearnContextFactory}) and
     * constructs {@link DLearnLabeledDataset}.
     *
     * @param featureExtractor feature extractor
     * @param lbExtractor label extractor
     * @param <K> type of keys in cache learning context
     * @param <V> type of values in cache learning context
     * @param <L> type of label
     * @return transformer
     */
    public static <K, V, L> CacheLabeledDatasetDLearnPartitionTransformer<K, V, L> cacheToLabeledDataset(
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return new CacheLabeledDatasetDLearnPartitionTransformer<>(featureExtractor, lbExtractor);
    }

    /**
     * Creates a transformer which accepts local learning context (produced by {@link LocalDLearnContextFactory}) and
     * constructs {@link DLearnDataset}.
     *
     * @param featureExtractor feature extractor
     * @param <K> type of keys in local learning context
     * @param <V> type of values in local learning context
     * @return transformer
     */
    public static <K, V> LocalDatasetDLearnPartitionTransformer<K, V> localToDataset(
        IgniteBiFunction<K, V, double[]> featureExtractor) {
        return new LocalDatasetDLearnPartitionTransformer<>(featureExtractor);
    }

    /**
     * Creates a transformer which accepts cache learning context (produced by {@link LocalDLearnContextFactory}) and
     * constructs {@link DLearnLabeledDataset}.
     *
     * @param featureExtractor feature extractor
     * @param lbExtractor label extractor
     * @param <K> type of keys in local learning context
     * @param <V> type of values in local learning context
     * @param <L> type of label
     * @return transformer
     */
    public static <K, V, L> LocalLabeledDatasetDLearnPartitionTransformer<K, V, L> localToLabeledDataset(
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return new LocalLabeledDatasetDLearnPartitionTransformer<>(featureExtractor, lbExtractor);
    }
}
