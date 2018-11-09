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

package org.apache.ignite.ml.preprocessing.imputing;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the imputing preprocessor.
 * The imputing fills the missed values according the imputing strategy (default: mean value for each feature).
 * It supports double values in features only.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class ImputerTrainer<K, V> implements PreprocessingTrainer<K, V, Vector, Vector> {
    /** The imputing strategy. */
    private ImputingStrategy imputingStgy = ImputingStrategy.MEAN;

    /** {@inheritDoc} */
    @Override public ImputerPreprocessor<K, V> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> basePreprocessor) {
        try (Dataset<EmptyContext, ImputerPartitionData> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                double[] sums = null;
                int[] counts = null;
                Map<Double, Integer>[] valuesByFreq = null;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Vector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    switch (imputingStgy) {
                        case MEAN:
                            sums = calculateTheSums(row, sums);
                            counts = calculateTheCounts(row, counts);
                            break;
                        case MOST_FREQUENT:
                            valuesByFreq = calculateFrequencies(row, valuesByFreq);
                            break;
                        default: throw new UnsupportedOperationException("The chosen strategy is not supported");
                    }
                }

                ImputerPartitionData partData;

                switch (imputingStgy) {
                    case MEAN:
                        partData = new ImputerPartitionData().withSums(sums).withCounts(counts);
                        break;
                    case MOST_FREQUENT:
                        partData = new ImputerPartitionData().withValuesByFrequency(valuesByFreq);
                        break;
                    default: throw new UnsupportedOperationException("The chosen strategy is not supported");
                }
                return partData;
            }
        )) {

            Vector imputingValues;

            switch (imputingStgy) {
                case MEAN:
                    imputingValues = VectorUtils.of(calculateImputingValuesBySumsAndCounts(dataset));
                    break;
                case MOST_FREQUENT:
                    imputingValues = VectorUtils.of(calculateImputingValuesByFrequencies(dataset));
                    break;
                default: throw new UnsupportedOperationException("The chosen strategy is not supported");
            }

            return new ImputerPreprocessor<>(imputingValues, basePreprocessor);

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculates the imputing values by frequencies keeping in the given dataset.
     *
     * @param dataset The dataset of frequencies for each feature aggregated in each partition..
     * @return Most frequent value for each feature.
     */
    private double[] calculateImputingValuesByFrequencies(
        Dataset<EmptyContext, ImputerPartitionData> dataset) {
        Map<Double, Integer>[] frequencies = dataset.compute(
            ImputerPartitionData::valuesByFrequency,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++) {
                    int finalI = i;
                    a[i].forEach((k, v) -> b[finalI].merge(k, v, (f1, f2) -> f1 + f2));
                }
                return b;
            }
        );

        double[] res = new double[frequencies.length];

        for (int i = 0; i < frequencies.length; i++) {
            Optional<Map.Entry<Double, Integer>> max = frequencies[i].entrySet()
                .stream()
                .max(Comparator.comparingInt(Map.Entry::getValue));

            if(max.isPresent())
                res[i] = max.get().getKey();
        }

        return res;
    }

    /**
     * Calculates the imputing values by sums and counts keeping in the given dataset.
     *
     * @param dataset The dataset with sums and counts for each feature aggregated in each partition.
     * @return The mean value for each feature.
     */
    private double[] calculateImputingValuesBySumsAndCounts(Dataset<EmptyContext, ImputerPartitionData> dataset) {
        double[] sums = dataset.compute(
            ImputerPartitionData::sums,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++)
                    a[i] += b[i];

                return a;
            }
        );

        int[] counts = dataset.compute(
            ImputerPartitionData::counts,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++)
                    a[i] += b[i];

                return a;
            }
        );

        double[] means = new double[sums.length];

        for (int i = 0; i < means.length; i++)
            means[i] = sums[i]/counts[i];

        return means;
    }

    /**
     * Updates frequencies by values and features.
     *
     * @param row Feature vector.
     * @param valuesByFreq Holds the sums by values and features.
     * @return Updated sums by values and features.
     */
    private Map<Double, Integer>[] calculateFrequencies(Vector row, Map<Double, Integer>[] valuesByFreq) {
        if (valuesByFreq == null) {
            valuesByFreq = new HashMap[row.size()];
            for (int i = 0; i < valuesByFreq.length; i++) valuesByFreq[i] = new HashMap<>();
        }
        else
            assert valuesByFreq.length == row.size() : "Base preprocessor must return exactly " + valuesByFreq.length
                + " features";

        for (int i = 0; i < valuesByFreq.length; i++) {
            double v = row.get(i);

            if(!Double.valueOf(v).equals(Double.NaN)) {
                Map<Double, Integer> map = valuesByFreq[i];

                if (map.containsKey(v))
                    map.put(v, (map.get(v)) + 1);
                else
                    map.put(v, 1);
            }
        }
        return valuesByFreq;
    }

    /**
     * Updates sums by features.
     *
     * @param row Feature vector.
     * @param sums Holds the sums by features.
     * @return Updated sums by features.
     */
    private double[] calculateTheSums(Vector row, double[] sums) {
        if (sums == null)
            sums = new double[row.size()];
        else
            assert sums.length == row.size() : "Base preprocessor must return exactly " + sums.length
                + " features";

        for (int i = 0; i < sums.length; i++){
            if(!Double.valueOf(row.get(i)).equals(Double.NaN))
                sums[i] += row.get(i);
        }

        return sums;
    }

    /**
     * Updates counts by features.
     *
     * @param row Feature vector.
     * @param counts Holds the counts by features.
     * @return Updated counts by features.
     */
    private int[] calculateTheCounts(Vector row, int[] counts) {
        if (counts == null)
            counts = new int[row.size()];
        else
            assert counts.length == row.size() : "Base preprocessor must return exactly " + counts.length
                + " features";

        for (int i = 0; i < counts.length; i++){
            if(!Double.valueOf(row.get(i)).equals(Double.NaN))
                counts[i]++;
        }

        return counts;
    }

    /**
     * Sets the imputing strategy.
     *
     * @param imputingStgy The given value.
     * @return The updated imputing trainer.
     */
    public ImputerTrainer<K, V> withImputingStrategy(ImputingStrategy imputingStgy){
        this.imputingStgy = imputingStgy;
        return this;
    }
}
