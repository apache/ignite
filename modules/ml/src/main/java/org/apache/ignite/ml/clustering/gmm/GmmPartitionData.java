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
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.math.functions.IgniteConsumer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.stat.MultivariateGaussianDistribution;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

class GmmPartitionData implements AutoCloseable {
    private List<LabeledVector<Double>> xs;
    private double[][] pcxi; //P(cluster|xi) where second idx is a cluster and first is a index of point

    GmmPartitionData(List<LabeledVector<Double>> xs, double[][] pcxi) {
        A.ensure(xs.size() == pcxi.length, "xs.size() == pcxi.length");

        this.xs = xs;
        this.pcxi = pcxi;
    }

    public List<LabeledVector<Double>> getXs() {
        return xs;
    }

    public double[][] getPcxi() {
        return pcxi;
    }

    public Vector getX(int i) {
        return xs.get(i).features();
    }

    public double pcxi(int cluster, int i) {
        return pcxi[i][cluster];
    }

    public int size() {
        return pcxi.length;
    }

    @Override public void close() throws Exception {
        //NOP
    }

    public static class Builder<K, V> implements PartitionDataBuilder<K, V, EmptyContext, GmmPartitionData> {
        private final FeatureLabelExtractor<K, V, Double> extractor;
        private final int countOfComponents;

        public Builder(FeatureLabelExtractor<K, V, Double> extractor, int countOfComponents) {
            this.extractor = extractor;
            this.countOfComponents = countOfComponents;
        }

        @Override public GmmPartitionData build(LearningEnvironment env, Iterator<UpstreamEntry<K, V>> upstreamData,
            long upstreamDataSize, EmptyContext ctx) {

            List<LabeledVector<Double>> xs = new ArrayList<>((int)upstreamDataSize);
            double[][] pcxi = new double[(int)upstreamDataSize][];
            int counter = 0;
            while (upstreamData.hasNext()) {
                UpstreamEntry<K, V> entry = upstreamData.next();
                LabeledVector<Double> x = extractor.extract(entry.getKey(), entry.getValue());
                xs.add(x);
                pcxi[counter] = new double[countOfComponents];
                pcxi[counter][env.randomNumbersGenerator().nextInt(countOfComponents)] = 1.0;
                counter++;
            }

            return new GmmPartitionData(xs, pcxi);
        }
    }

    public static IgniteConsumer<GmmPartitionData> updatePcxiMapper(Vector clusterProbs,
        List<MultivariateGaussianDistribution> components) {

        return data -> {
            for (int i = 0; i < data.size(); i++) {
                Vector x = data.getX(i);
                double normalizer = 0.0;
                for (int c = 0; c < clusterProbs.size(); c++)
                    normalizer += components.get(c).prob(x) * clusterProbs.get(c);

                for (int c = 0; c < clusterProbs.size(); c++)
                    data.pcxi[i][c] = (components.get(c).prob(x) * clusterProbs.get(c)) / normalizer;
            }
        };
    }
}
