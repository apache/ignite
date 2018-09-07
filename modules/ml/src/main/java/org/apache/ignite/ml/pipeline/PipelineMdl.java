package org.apache.ignite.ml.pipeline;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public class PipelineMdl<K, V, L> implements Model<Vector,L> {
    private Model<Vector, L> internalMdl;
    private IgniteBiFunction<K, V, Vector> featureExtractor;
    private IgniteBiFunction<K, V, Double> lbExtractor;

    @Override public L apply(Vector vector) {
        return internalMdl.apply(vector);
    }

    public IgniteBiFunction<K, V, Vector> getFeatureExtractor() {
        return featureExtractor;
    }

    public IgniteBiFunction<K, V, Double>  getLabelExtractor() {
        return lbExtractor;
    }

    public PipelineMdl<K, V, L> withInternalMdl(Model<Vector, L> internalMdl){
        this.internalMdl = internalMdl;
        return this;
    }

    public PipelineMdl<K, V, L> withFeatureExtractor(IgniteBiFunction<K, V, Vector>  featureExtractor){
        this.featureExtractor = featureExtractor;
        return this;
    }

    public PipelineMdl<K, V, L> withLabelExtractor(IgniteBiFunction<K, V, Double> lbExtractor){
        this.lbExtractor = lbExtractor;
        return this;
    }

    @Override public String toString() {
        return "PipelineMdl{" +
            "internalMdl=" + internalMdl +
            '}';
    }
}
