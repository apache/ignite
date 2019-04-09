package org.apache.ignite.ml.preprocessing;

import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.structures.LabeledVector;

public interface Preprocessor<K, V> extends IgniteBiFunction<K, V, LabeledVector> {
}
