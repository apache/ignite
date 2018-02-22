package org.apache.ignite.ml.knn.models;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.isolve.LinSysPartitionDataOnHeap;

public class KNNPartitionDataBuilderOnHeap<K, V, C extends Serializable>
    implements PartitionDataBuilder<K, V, C, KNNPartitionDataOnHeap> {
    /** */
    private static final long serialVersionUID = -7820760153954269227L;

    /** Extractor of X matrix row. */
    private final IgniteBiFunction<K, V, double[]> xExtractor;

    /** Extractor of Y vector value. */
    private final IgniteBiFunction<K, V, Double> yExtractor;

    /** Number of columns. */
    private final int cols;

    /**
     * Constructs a new instance of linear system partition data builder.
     *
     * @param xExtractor Extractor of X matrix row.
     * @param yExtractor Extractor of Y vector value.
     * @param cols Number of columns.
     */
    public KNNPartitionDataBuilderOnHeap(IgniteBiFunction<K, V, double[]> xExtractor,
        IgniteBiFunction<K, V, Double> yExtractor, int cols) {
        this.xExtractor = xExtractor;
        this.yExtractor = yExtractor;
        this.cols = cols;
    }

    /** {@inheritDoc} */
    @Override public KNNPartitionDataOnHeap build(Iterator<UpstreamEntry<K, V>> upstreamData, long upstreamDataSize,
        C ctx) {
        // Prepares the matrix of features in flat column-major format.
        double[][] x = new double[Math.toIntExact(upstreamDataSize)][cols];
        double[] y = new double[Math.toIntExact(upstreamDataSize)];

        int ptr = 0;
        while (upstreamData.hasNext()) {
            UpstreamEntry<K, V> entry = upstreamData.next();
            double[] row = xExtractor.apply(entry.getKey(), entry.getValue());

            assert row.length == cols : "X extractor must return exactly " + cols + " columns";

            x[ptr] = row;

            y[ptr] = yExtractor.apply(entry.getKey(), entry.getValue());

            ptr++;
        }

        return new KNNPartitionDataOnHeap(x, Math.toIntExact(upstreamDataSize), cols, y);
    }
}
