/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.clustering.gmm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Partition data for GMM algorithm. Unlike partition data for other algorithms this class aggregate probabilities of
 * each cluster of gaussians mixture (see {@link #pcxi}) for each vector in dataset.
 */
class GmmPartitionData implements AutoCloseable {
    /** Dataset vectors. */
    private List<LabeledVector<Double>> xs;

    /** P(cluster|xi) where second idx is a cluster and first is a index of point. */
    private double[][] pcxi;

    /**
     * Creates an instance of GmmPartitionData.
     *
     * @param xs Dataset.
     * @param pcxi P(cluster|xi) per cluster.
     */
    GmmPartitionData(List<LabeledVector<Double>> xs, double[][] pcxi) {
        A.ensure(xs.size() == pcxi.length, "xs.size() == pcxi.length");

        this.xs = xs;
        this.pcxi = pcxi;
    }

    /**
     * @param i Index of vector in partition.
     * @return Vector.
     */
    public Vector getX(int i) {
        return xs.get(i).features();
    }

    /**
     * Updates P(c|xi) values in partitions and compute dataset likelihood.
     *
     * @param dataset Dataset.
     * @param clusterProbs Component probabilities.
     * @param components Components.
     * @return Dataset likelihood.
     */
    static double updatePcxiAndComputeLikelihood(Dataset<EmptyContext, GmmPartitionData> dataset, Vector clusterProbs,
        List<MultivariateGaussianDistribution> components) {

        return dataset.compute(
            data -> updatePcxi(data, clusterProbs, components),
            (left, right) -> asPrimitive(left) + asPrimitive(right)
        );
    }

    /**
     * @param cluster Cluster id.
     * @param i Vector id.
     * @return P(cluster | xi) value.
     */
    public double pcxi(int cluster, int i) {
        return pcxi[i][cluster];
    }

    /**
     * @param cluster Cluster id.
     * @param i Vector id.
     * @param value P(cluster|xi) value.
     */
    public void setPcxi(int cluster, int i, double value) {
        pcxi[i][cluster] = value;
    }

    /**
     * @return All vectors from partition.
     */
    public List<LabeledVector<Double>> getAllXs() {
        return Collections.unmodifiableList(xs);
    }

    /** {@inheritDoc} */
    @Override public void close() throws Exception {
        //NOP
    }

    /**
     * Builder for GMM partition data.
     */
    public static class Builder<K, V> implements PartitionDataBuilder<K, V, EmptyContext, GmmPartitionData> {
        /** Serial version uid. */
        private static final long serialVersionUID = 1847063348042022561L;

        /** Upsteam vectorizer. */
        private final Preprocessor<K, V> preprocessor;

        /** Count of components of mixture. */
        private final int countOfComponents;

        /**
         * Creates an instance of Builder.
         *
         * @param preprocessor preprocessor.
         * @param countOfComponents Count of components.
         */
        public Builder(Preprocessor<K, V> preprocessor, int countOfComponents) {
            this.preprocessor = preprocessor;
            this.countOfComponents = countOfComponents;
        }

        /** {@inheritDoc} */
        @Override public GmmPartitionData build(LearningEnvironment env, Iterator<UpstreamEntry<K, V>> upstreamData,
            long upstreamDataSize, EmptyContext ctx) {

            int rowsCount = Math.toIntExact(upstreamDataSize);
            List<LabeledVector<Double>> xs = new ArrayList<>(rowsCount);
            double[][] pcxi = new double[rowsCount][countOfComponents];

            while (upstreamData.hasNext()) {
                UpstreamEntry<K, V> entry = upstreamData.next();
                LabeledVector<Double> x = preprocessor.apply(entry.getKey(), entry.getValue());
                xs.add(x);
            }

            return new GmmPartitionData(xs, pcxi);
        }
    }

    /**
     * Sets P(c|xi) = 1 for closest cluster "c" for each vector in partition data using initial means as cluster centers
     * (like in k-means).
     *
     * @param initMeans Initial means.
     */
    static void estimateLikelihoodClusters(GmmPartitionData data, Vector[] initMeans) {
        for (int i = 0; i < data.size(); i++) {
            int closestClusterId = -1;
            double minSquaredDist = Double.MAX_VALUE;

            Vector x = data.getX(i);
            for (int c = 0; c < initMeans.length; c++) {
                data.setPcxi(c, i, 0.0);
                double distance = initMeans[c].getDistanceSquared(x);
                if (distance < minSquaredDist) {
                    closestClusterId = c;
                    minSquaredDist = distance;
                }
            }

            data.setPcxi(closestClusterId, i, 1.);
        }
    }

    /**
     * @return Size of dataset partition.
     */
    public int size() {
        return pcxi.length;
    }

    /**
     * Updates P(c|xi) values in partitions given components probabilities and components of GMM.
     *
     * @param clusterProbs Component probabilities.
     * @param components Components.
     */
    static double updatePcxi(GmmPartitionData data, Vector clusterProbs,
        List<MultivariateGaussianDistribution> components) {

        GmmModel model = new GmmModel(clusterProbs, components);
        double maxProb = Double.NEGATIVE_INFINITY;

        for (int i = 0; i < data.size(); i++) {
            Vector x = data.getX(i);
            double xProb = model.prob(x);
            if (xProb > maxProb)
                maxProb = xProb;

            double normalizer = 0.0;
            for (int c = 0; c < clusterProbs.size(); c++)
                normalizer += components.get(c).prob(x) * clusterProbs.get(c);

            for (int c = 0; c < clusterProbs.size(); c++)
                data.pcxi[i][c] = (components.get(c).prob(x) * clusterProbs.get(c)) / normalizer;
        }

        return maxProb;
    }

    /**
     * @param val Value.
     * @return 0 if Value == null and simplified value in terms of type otherwise.
     */
    private static double asPrimitive(Double val) {
        return val == null ? 0.0 : val;
    }
}
