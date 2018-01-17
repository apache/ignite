package org.apache.ignite.ml.dlearn.context.transformer;

import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionFactory;

/**
 * Transformer which allow to transform one learning context into another. Transformation mean that new d-learn
 * partitions will be created from old partitions and saved in the same underlying storage, but with a new learning
 * context id. New partitions will be containing a new data required to provide new API as a new learning context.
 *
 * All generic transformers are aggregated in {@link DLearnContextTransformers}.
 *
 * @param <P> type of an initial partition
 * @param <T> type of a new partition
 * @param <C> type of a new learning context
 */
public interface DLearnContextTransformer<P extends AutoCloseable, T extends AutoCloseable, C extends DLearnContext<T>>
    extends DLearnPartitionFactory<T> {
    /**
     * Copies required data from old partition into new one. All needed transformations are allowed.
     *
     * @param oldPart old (initial) partition
     * @param newPart new partition
     */
    public void transform(P oldPart, T newPart);

    /**
     * Wraps learning context to provide partition-specific API.
     *
     * @param ctx context
     * @return wrapped context
     */
    public C wrapContext(DLearnContext<T> ctx);
}
