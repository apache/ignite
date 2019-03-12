package org.apache.ignite.ml.preprocessing.dct;

import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public class DiscreteCosinePreprocessor<K, V> implements IgniteBiFunction<K, V, Vector> {

    private final IgniteBiFunction<K, V, Vector> basePreprocessor;

    public DiscreteCosinePreprocessor(IgniteBiFunction<K, V, Vector> basePreprocessor) {
        this.basePreprocessor = basePreprocessor;
    }

    @Override
    public Vector apply(K k, V v) {
        Vector init = basePreprocessor.apply(k, v);

        Vector res = init.like(init.size());

        for (int i = 0; i < init.size(); i++) {
            int sum = 0;

            for (int j = 0; j < init.size(); j++)
                sum += init.get(j) * Math.cos((Math.PI/init.size()) * (j + 0.5) * i);

            res.set(i, sum);
        }

        return res;
    }
}
