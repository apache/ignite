package org.apache.ignite.ml.dlearn.context.transformer;

import org.apache.ignite.ml.dlearn.DLearnContext;
import org.apache.ignite.ml.dlearn.DLearnPartitionFactory;

/** */
public interface DLearnContextTransformer<P, T, C extends DLearnContext<T>> extends DLearnPartitionFactory<T> {
    /** */
    public void transform(P oldPart, T newPart);

    /** */
    public C wrapContext(DLearnContext<T> ctx);
}
