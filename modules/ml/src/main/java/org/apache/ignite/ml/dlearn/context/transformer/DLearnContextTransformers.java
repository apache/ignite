package org.apache.ignite.ml.dlearn.context.transformer;

import org.apache.ignite.ml.dlearn.context.cache.CacheDLearnPartition;
import org.apache.ignite.ml.dlearn.context.local.LocalDLearnPartition;
import org.apache.ignite.ml.dlearn.part.DatasetDLeanPartition;
import org.apache.ignite.ml.dlearn.part.LabeledDatasetDLearnPartition;
import org.apache.ignite.ml.dlearn.context.transformer.cache.CacheDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.cache.CacheLabeledDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.local.LocalDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.context.transformer.local.LocalLabeledDatasetDLearnPartitionTransformer;
import org.apache.ignite.ml.dlearn.utils.DLearnContextTransformer;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteFunction;

/** */
public class DLearnContextTransformers {
    /** */
    public static <K, V> DLearnContextTransformer<CacheDLearnPartition<K, V>, DatasetDLeanPartition> cacheToDataset(IgniteBiFunction<K, V, double[]> featureExtractor) {
        return new DLearnContextTransformer<>(
            new CacheDatasetDLearnPartitionTransformer<>(featureExtractor),
            DatasetDLeanPartition::new
        );
    }

    /** */
    public static <K, V, L> DLearnContextTransformer<CacheDLearnPartition<K, V>, LabeledDatasetDLearnPartition<L>> cacheToLabeledDataset(IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return new DLearnContextTransformer<>(
            new CacheLabeledDatasetDLearnPartitionTransformer<>(featureExtractor, lbExtractor),
            LabeledDatasetDLearnPartition::new
        );
    }

    /** */
    public static <V> DLearnContextTransformer<LocalDLearnPartition<V>, DatasetDLeanPartition> localToDataset(IgniteFunction<V, double[]> featureExtractor) {
        return new DLearnContextTransformer<>(
            new LocalDatasetDLearnPartitionTransformer<>(featureExtractor),
            DatasetDLeanPartition::new
        );
    }

    /** */
    public static <V, L> DLearnContextTransformer<LocalDLearnPartition<V>, LabeledDatasetDLearnPartition<L>> localToLabeledDataset(IgniteFunction<V, double[]> featureExtractor, IgniteFunction<V, L> lbExtractor) {
        return new DLearnContextTransformer<>(
            new LocalLabeledDatasetDLearnPartitionTransformer<>(featureExtractor, lbExtractor),
            LabeledDatasetDLearnPartition::new
        );
    }
}
