package org.apache.ignite.ml.dlearn.context.transformer;

import org.apache.ignite.ml.dlearn.context.transformer.cache.CacheDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.cache.CacheLabeledDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.local.LocalDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.local.LocalLabeledDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;

/** */
public class DLearnContextTransformers {
    /** */
    public static <K, V> CacheDatasetDLearnPartitionTransformer<K, V> cacheToDataset(IgniteBiFunction<K, V, double[]> featureExtractor) {
        return new CacheDatasetDLearnPartitionTransformer<>(featureExtractor);
    }

    /** */
    public static <K, V, L> CacheLabeledDatasetDLearnPartitionTransformer<K, V, L> cacheToLabeledDataset(IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return new CacheLabeledDatasetDLearnPartitionTransformer<>(featureExtractor, lbExtractor);
    }

    /** */
    public static <K, V> LocalDatasetDLearnPartitionTransformer<K, V> localToDataset(IgniteBiFunction<K, V, double[]> featureExtractor) {
        return new LocalDatasetDLearnPartitionTransformer<>(featureExtractor);
    }

    /** */
    public static <K, V, L> LocalLabeledDatasetDLearnPartitionTransformer<K, V, L> localToLabeledDataset(IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return new LocalLabeledDatasetDLearnPartitionTransformer<>(featureExtractor, lbExtractor);
    }
}
