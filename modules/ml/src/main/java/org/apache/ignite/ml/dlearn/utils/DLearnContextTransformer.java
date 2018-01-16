package org.apache.ignite.ml.dlearn.utils;

import org.apache.ignite.ml.dlearn.DLearnPartitionFactory;
import org.apache.ignite.ml.math.functions.IgniteBiConsumer;

/** */
public class DLearnContextTransformer<P, T> {
    /** */
    private final IgniteBiConsumer<P, T> transformer;

    /** */
    private final DLearnPartitionFactory<T> partFactory;

    /** */
    public DLearnContextTransformer(IgniteBiConsumer<P, T> transformer, DLearnPartitionFactory<T> partFactory) {
        this.transformer = transformer;
        this.partFactory = partFactory;
    }

    /** */
    public IgniteBiConsumer<P, T> getTransformer() {
        return transformer;
    }

    /** */
    public DLearnPartitionFactory<T> getPartFactory() {
        return partFactory;
    }
}
