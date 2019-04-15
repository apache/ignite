package org.apache.ignite.ml.preprocessing.developer;

import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 *
 *
 * @param <K> Type of key.
 * @param <V> Type of value.
 * @param <L0> Type of original label.
 * @param <L1> Type of mapped label.
 */
public class MappedPreprocessor<K, V, L0, L1> implements Preprocessor<K, V> {
    /** Original preprocessor. */
    protected final Preprocessor<K, V> original;

    /** Vectors mapping. */
    private final IgniteFunction<LabeledVector<L0>, LabeledVector<L1>> mapping;

    /**
     * Creates an instance of MappedPreprocessor.
     */
    public MappedPreprocessor(Preprocessor<K, V> original,
        IgniteFunction<LabeledVector<L0>, LabeledVector<L1>> andThen) {

        this.original = original;
        this.mapping = andThen;
    }

    /** {@inheritDoc} */
    @Override public LabeledVector<L1> apply(K key, V value) {
        LabeledVector<L0> origVec = original.apply(key, value);
        return mapping.apply(origVec);
    }
}
