package org.apache.ignite.ml.idd;

import org.apache.ignite.ml.math.functions.IgniteConsumer;

public interface IDD<K, V, D, L> {

    public void compute(IgniteConsumer<IDDPartition<K, V, D, L>> consumer);
}
