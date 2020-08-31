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
import org.apache.ignite.ml.dataset.PartitionContextBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Trainer of the imputing preprocessor.
 * The imputing fills the missed values according the imputing strategy (default: mean value for each feature).
 * It supports double values in features only.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class ImputerTrainer<K, V> implements PreprocessingTrainer<K, V> {
    /** The imputing strategy. */
    private ImputingStrategy imputingStgy = ImputingStrategy.MEAN;

    /** {@inheritDoc} */
    @Override public ImputerPreprocessor<K, V> fit(LearningEnvironmentBuilder envBuilder, DatasetBuilder<K, V> datasetBuilder,
                                                   Preprocessor<K, V> basePreprocessor) {
        PartitionContextBuilder<K, V, EmptyContext> builder = (env, upstream, upstreamSize) -> new EmptyContext();
        try (Dataset<EmptyContext, ImputerPartitionData> dataset = datasetBuilder.build(
            envBuilder,
            builder,
            (env, upstream, upstreamSize, ctx) -> {
                double[] sums = null;
                int[] counts = null;
                double[] maxs = null;
                double[] mins = null;
                Map<Double, Integer>[] valuesByFreq = null;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    LabeledVector row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                    switch (imputingStgy) {
                        case MEAN:
                            sums = updateTheSums(row, sums);
                            counts = updateTheCounts(row, counts);
                            break;
                        case MOST_FREQUENT:
                            valuesByFreq = updateFrequenciesByGivenRow(row, valuesByFreq);
                            break;
                        case LEAST_FREQUENT:
                            valuesByFreq = updateFrequenciesByGivenRow(row, valuesByFreq);
                            break;
                        case MAX:
                            maxs = updateTheMaxs(row, maxs);
                            break;
                        case MIN:
                            mins = updateTheMins(row, mins);
                            break;
                        case COUNT:
                            counts = updateTheCounts(row, counts);
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
                    case LEAST_FREQUENT:
                        partData = new ImputerPartitionData().withValuesByFrequency(valuesByFreq);
                        break;
                    case MAX:
                        partData = new ImputerPartitionData().withMaxs(maxs);
                        break;
                    case MIN:
                        partData = new ImputerPartitionData().withMins(mins);
                        break;
                    case COUNT:
                        partData = new ImputerPartitionData().withCounts(counts);
                        break;
                    default: throw new UnsupportedOperationException("The chosen strategy is not supported");
                }
                return partData;
            }, learningEnvironment(basePreprocessor)
        )) {

            Vector imputingValues;

            switch (imputingStgy) {
                case MEAN:
                    imputingValues = VectorUtils.of(calculateImputingValuesBySumsAndCounts(dataset));
                    break;
                case MOST_FREQUENT:
                    imputingValues = VectorUtils.of(calculateImputingValuesByTheMostFrequentValues(dataset));
                    break;
                case LEAST_FREQUENT:
                    imputingValues = VectorUtils.of(calculateImputingValuesByTheLeastFrequentValues(dataset));
                    break;
                case MAX:
                    imputingValues = VectorUtils.of(calculateImputingValuesByMaxValues(dataset));
                    break;
                case MIN:
                    imputingValues = VectorUtils.of(calculateImputingValuesByMinValues(dataset));
                    break;
                case COUNT:
                    imputingValues = VectorUtils.of(calculateImputingValuesByCounts(dataset));
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
    private double[] calculateImputingValuesByTheMostFrequentValues(
        Dataset<EmptyContext, ImputerPartitionData> dataset) {
        Map<Double, Integer>[] frequencies = getAggregatedFrequencies(dataset);

        double[] res = new double[frequencies.length];

        for (int i = 0; i < frequencies.length; i++) {
            Optional<Map.Entry<Double, Integer>> max = frequencies[i].entrySet()
                .stream()
                .max(Comparator.comparingInt(Map.Entry::getValue));

            if (max.isPresent())
                res[i] = max.get().getKey();
        }

        return res;
    }

    /**
     * Calculates the imputing values by frequencies keeping in the given dataset.
     *
     * @param dataset The dataset of frequencies for each feature aggregated in each partition..
     * @return Least frequent value for each feature.
     */
    private double[] calculateImputingValuesByTheLeastFrequentValues(
        Dataset<EmptyContext, ImputerPartitionData> dataset) {
        Map<Double, Integer>[] frequencies = getAggregatedFrequencies(dataset);

        double[] res = new double[frequencies.length];

        for (int i = 0; i < frequencies.length; i++) {
            Optional<Map.Entry<Double, Integer>> max = frequencies[i].entrySet()
                .stream()
                .min(Comparator.comparingInt(Map.Entry::getValue));

            if (max.isPresent())
                res[i] = max.get().getKey();
        }

        return res;
    }

    /**
     * Merges the frequencies from each partition to the aggregated common state.
     *
     * @param dataset Dataset.
     */
    private Map<Double, Integer>[] getAggregatedFrequencies(Dataset<EmptyContext, ImputerPartitionData> dataset) {
        return dataset.compute(
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
    }

    /**
     * Calculates the imputing values by counts keeping in the given dataset.
     *
     * @param dataset The dataset with counts for each feature aggregated in each partition.
     * @return The count value for each feature.
     */
    private double[] calculateImputingValuesByCounts(Dataset<EmptyContext, ImputerPartitionData> dataset) {
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

        // convert to double array
        double[] res = new double[counts.length];

        for (int i = 0; i < res.length; i++)
            res[i] = counts[i];

        return res;
    }

    /**
     * Calculates the imputing values by min values keeping in the given dataset.
     *
     * @param dataset The dataset with min values for each feature aggregated in each partition.
     * @return The min value for each feature.
     */
    private double[] calculateImputingValuesByMinValues(Dataset<EmptyContext, ImputerPartitionData> dataset) {
        return dataset.compute(
            ImputerPartitionData::mins,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++)
                    a[i] = Math.min(a[i], b[i]);

                return a;
            }
        );
    }

    /**
     * Calculates the imputing values by max values keeping in the given dataset.
     *
     * @param dataset The dataset with max values for each feature aggregated in each partition.
     * @return The max value for each feature.
     */
    private double[] calculateImputingValuesByMaxValues(Dataset<EmptyContext, ImputerPartitionData> dataset) {
        return dataset.compute(
            ImputerPartitionData::maxs,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++)
                    a[i] = Math.max(a[i], b[i]);

                return a;
            }
        );
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
            means[i] = sums[i] / counts[i];

        return means;
    }

    /**
     * Updates frequencies by values and features.
     *
     * @param row Feature vector.
     * @param valuesByFreq Holds the sums by values and features.
     * @return Updated sums by values and features.
     */
    private Map<Double, Integer>[] updateFrequenciesByGivenRow(LabeledVector row, Map<Double, Integer>[] valuesByFreq) {
        if (valuesByFreq == null) {
            valuesByFreq = new HashMap[row.size()];
            for (int i = 0; i < valuesByFreq.length; i++) valuesByFreq[i] = new HashMap<>();
        }
        else
            assert valuesByFreq.length == row.size() : "Base preprocessor must return exactly " + valuesByFreq.length
                + " features";

        for (int i = 0; i < valuesByFreq.length; i++) {
            double v = row.get(i);

            if (!Double.valueOf(v).equals(Double.NaN)) {
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
    private double[] updateTheSums(LabeledVector row, double[] sums) {
        if (sums == null)
            sums = new double[row.size()];
        else
            assert sums.length == row.size() : "Base preprocessor must return exactly " + sums.length
                + " features";

        for (int i = 0; i < sums.length; i++) {
            if (!Double.valueOf(row.get(i)).equals(Double.NaN))
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
    private int[] updateTheCounts(LabeledVector row, int[] counts) {
        if (counts == null)
            counts = new int[row.size()];
        else
            assert counts.length == row.size() : "Base preprocessor must return exactly " + counts.length
                + " features";

        for (int i = 0; i < counts.length; i++) {
            if (!Double.valueOf(row.get(i)).equals(Double.NaN))
                counts[i]++;
        }

        return counts;
    }

    /**
     * Updates mins by features.
     *
     * @param row Feature vector.
     * @param mins Holds the mins by features.
     * @return Updated mins by features.
     */
    private double[] updateTheMins(LabeledVector row, double[] mins) {
        if (mins == null) {
            mins = new double[row.size()];
            for (int i = 0; i < mins.length; i++)
                mins[i] = Double.POSITIVE_INFINITY;
        }

        else
            assert mins.length == row.size() : "Base preprocessor must return exactly " + mins.length
                + " features";

        for (int i = 0; i < mins.length; i++) {
            if (!Double.valueOf(row.get(i)).equals(Double.NaN))
                mins[i] = Math.min(mins[i], row.get(i));
        }

        return mins;
    }

    /**
     * Updates maxs by features.
     *
     * @param row Feature vector.
     * @param maxs Holds the maxs by features.
     * @return Updated maxs by features.
     */
    private double[] updateTheMaxs(LabeledVector row, double[] maxs) {
        if (maxs == null) {
            maxs = new double[row.size()];
            for (int i = 0; i < maxs.length; i++)
                maxs[i] = Double.NEGATIVE_INFINITY;
        }

        else
            assert maxs.length == row.size() : "Base preprocessor must return exactly " + maxs.length
                + " features";

        for (int i = 0; i < maxs.length; i++) {
            if (!Double.valueOf(row.get(i)).equals(Double.NaN))
                maxs[i] = Math.max(maxs[i], row.get(i));
        }

        return maxs;
    }

    /**
     * Sets the imputing strategy.
     *
     * @param imputingStgy The given value.
     * @return The updated imputing trainer.
     */
    public ImputerTrainer<K, V> withImputingStrategy(ImputingStrategy imputingStgy) {
        this.imputingStgy = imputingStgy;
        return this;
    }
}
