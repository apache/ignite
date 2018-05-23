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

package org.apache.ignite.ml.preprocessing.imputer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;

/**
 * Trainer of the normalization preprocessor.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class ImputerTrainer<K, V> implements PreprocessingTrainer<K, V, double[], double[]> {
    /** Threshold. */
    private ImputingStrategy imputingStrategy = ImputingStrategy.MEAN;

    /** Throw an exception on invalid data if false or miss otherwise. */
    private boolean isMissingInvalidData = true;

    /** {@inheritDoc} */
    @Override public ImputerPreprocessor<K, V> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> basePreprocessor) {
        try (Dataset<EmptyContext, ImputerPartitionData> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                double[] sums = null;
                int[] counts = null;
                Map<Double, Integer>[] valuesByFreq = null;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    double[] row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    switch (imputingStrategy) {
                        case MEAN:
                            sums = calculateTheSums(row, sums);
                            counts = calculateTheCounts(row, counts);
                            break;
                        case MOST_FREQUENT:
                            valuesByFreq = calculateFrequencies(row, valuesByFreq);
                            break;
                    }
                }

                ImputerPartitionData partData = null;

                switch (imputingStrategy) {
                    case MEAN:
                        partData = new ImputerPartitionData().withSums(sums).withCounts(counts);
                        break;
                    case MOST_FREQUENT:
                        partData = new ImputerPartitionData().withValuesByFrequency(valuesByFreq);
                        break;
                    case EMPTY:
                        break;
                }
                return partData;
            }
        )) {

            double[] imputingValues = null;

            switch (imputingStrategy) {
                case MEAN:
                    imputingValues = calculateImputingValuesBySums(dataset);
                    break;
                case MOST_FREQUENT:
                    imputingValues = calculateImputingValuesByFrequencies(dataset);
                    break;
                case EMPTY:
                    break;
            }

            return new ImputerPreprocessor<>(imputingValues, basePreprocessor);

        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

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
                return a;
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

    private double[] calculateImputingValuesBySums(Dataset<EmptyContext, ImputerPartitionData> dataset) {
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
    private Map<Double, Integer>[] calculateFrequencies(double[] row, Map<Double, Integer>[] valuesByFreq) {
        if (valuesByFreq == null)
            valuesByFreq = new HashMap[row.length];
        else
            assert valuesByFreq.length == row.length : "Base preprocessor must return exactly " + valuesByFreq.length
                + " features";

        for (int i = 0; i < valuesByFreq.length; i++) {
            double v = row[i];

            if(!Double.valueOf(v).equals(Double.NaN)) {
                if (valuesByFreq[i] == null)
                    valuesByFreq[i] = new HashMap<>();

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
    private double[] calculateTheSums(double[] row, double[] sums) {
        if (sums == null)
            sums = new double[row.length];
        else
            assert sums.length == row.length : "Base preprocessor must return exactly " + sums.length
                + " features";

        for (int i = 0; i < sums.length; i++){
            if(!Double.valueOf(row[i]).equals(Double.NaN))
                sums[i] += row[i];
        }

        return sums;
    }

    /**
     * Updates sums by features.
     *
     * @param row Feature vector.
     * @param counts Holds the sums by features.
     * @return Updated sums by features.
     */
    private int[] calculateTheCounts(double[] row, int[] counts) {
        if (counts == null)
            counts = new int[row.length];
        else
            assert counts.length == row.length : "Base preprocessor must return exactly " + counts.length
                + " features";

        for (int i = 0; i < counts.length; i++){
            if(!Double.valueOf(row[i]).equals(Double.NaN))
                counts[i]++;
        }

        return counts;
    }

    public ImputerTrainer<K, V> withImputingStrategy(ImputingStrategy imputingStrategy){
        this.imputingStrategy = imputingStrategy;
        return this;
    }
}
