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

package org.apache.ignite.ml.preprocessing.encoding;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.UpstreamEntry;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.exceptions.preprocessing.UndefinedLabelException;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.frequency.FrequencyEncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.label.LabelEncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.onehotencoder.OneHotEncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderPreprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;

/**
 * Trainer of the String Encoder and One-Hot Encoder preprocessors.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class EncoderTrainer<K, V> implements PreprocessingTrainer<K, V> {
    /** Indices of features which should be encoded. */
    private Set<Integer> handledIndices = new HashSet<>();

    /** Encoder preprocessor type. */
    private EncoderType encoderType = EncoderType.ONE_HOT_ENCODER;

    /** Encoder sorting strategy. */
    private EncoderSortingStrategy encoderSortingStgy = EncoderSortingStrategy.FREQUENCY_DESC;

    /** {@inheritDoc} */
    @Override public EncoderPreprocessor<K, V> fit(
        LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder,
        Preprocessor<K, V> basePreprocessor) {
        if (handledIndices.isEmpty() && encoderType != EncoderType.LABEL_ENCODER)
            throw new RuntimeException("Add indices of handled features");

        try (Dataset<EmptyContext, EncoderPartitionData> dataset = datasetBuilder.build(
            envBuilder,
            (env, upstream, upstreamSize) -> new EmptyContext(),
            (env, upstream, upstreamSize, ctx) -> {
                EncoderPartitionData partData = new EncoderPartitionData();

                if (encoderType == EncoderType.LABEL_ENCODER) {
                    Map<String, Integer> lbFrequencies = null;

                    while (upstream.hasNext()) {
                        UpstreamEntry<K, V> entity = upstream.next();
                        LabeledVector<Double> row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                        lbFrequencies = updateLabelFrequenciesForNextRow(row, lbFrequencies);
                    }

                    partData.withLabelFrequencies(lbFrequencies);
                }
                else {
                    // This array will contain not null values for handled indices
                    Map<String, Integer>[] categoryFrequencies = null;

                    while (upstream.hasNext()) {
                        UpstreamEntry<K, V> entity = upstream.next();
                        LabeledVector<Double> row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                        categoryFrequencies = updateFeatureFrequenciesForNextRow(row, categoryFrequencies);
                    }
                    partData.withCategoryFrequencies(categoryFrequencies);
                }

                return partData;
            }, learningEnvironment(basePreprocessor)
        )) {
            switch (encoderType) {
                case ONE_HOT_ENCODER:
                    return new OneHotEncoderPreprocessor<>(calculateEncodingValuesByFrequencies(dataset), basePreprocessor, handledIndices);
                case STRING_ENCODER:
                    return new StringEncoderPreprocessor<>(calculateEncodingValuesByFrequencies(dataset), basePreprocessor, handledIndices);
                case LABEL_ENCODER:
                    return new LabelEncoderPreprocessor<>(calculateEncodingValuesForLabelsByFrequencies(dataset), basePreprocessor);
                case FREQUENCY_ENCODER:
                    return new FrequencyEncoderPreprocessor<>(calculateEncodingFrequencies(dataset), basePreprocessor, handledIndices);
                default:
                    throw new IllegalStateException("Define the type of the resulting prerocessor.");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculates encoding frequencies as frequency divided on amount of rows in dataset.
     *
     * NOTE: The amount of rows is calculated as sum of absolute frequencies.
     *
     * @param dataset Dataset.
     * @return Encoding frequency for each feature.
     */
    private Map<String, Double>[] calculateEncodingFrequencies(Dataset<EmptyContext, EncoderPartitionData> dataset) {
        Map<String, Integer>[] frequencies = calculateFrequencies(dataset);

        Map<String, Double>[] res = new Map[frequencies.length];

        int[] counters = new int[frequencies.length];

        for (int i = 0; i < frequencies.length; i++) {
            counters[i] = frequencies[i].values().stream().reduce(0, Integer::sum);
            int locI = i;
            res[locI] = new HashMap<>();
            frequencies[i].forEach((k, v) -> res[locI].put(k, (double)v / counters[locI]));
        }

        return res;
    }

    /**
     * Calculates frequencies for each feature.
     *
     * @param dataset Dataset.
     * @return Frequency for each feature.
     */
    private Map<String, Integer>[] calculateFrequencies(Dataset<EmptyContext, EncoderPartitionData> dataset) {
        return dataset.compute(
            EncoderPartitionData::categoryFrequencies,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++) {
                    if (handledIndices.contains(i)) {
                        int finalI = i;
                        a[i].forEach((k, v) -> b[finalI].merge(k, v, (f1, f2) -> f1 + f2));
                    }
                }
                return b;
            }
        );
    }

    /**
     * Calculates frequencies for labels.
     *
     * @param dataset Dataset.
     * @return Frequency for labels.
     */
    private Map<String, Integer> calculateFrequenciesForLabels(Dataset<EmptyContext, EncoderPartitionData> dataset) {
        return dataset.compute(
            EncoderPartitionData::labelFrequencies,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                a.forEach((k, v) -> b.merge(k, v, (f1, f2) -> f1 + f2));

                return b;
            }
        );
    }

    /**
     * Calculates the encoding values values by frequencies keeping in the given dataset.
     *
     * @param dataset The dataset of frequencies for each feature aggregated in each partition.
     * @return Encoding values for each feature.
     */
    private Map<String, Integer>[] calculateEncodingValuesByFrequencies(
        Dataset<EmptyContext, EncoderPartitionData> dataset) {
        Map<String, Integer>[] frequencies = calculateFrequencies(dataset);

        Map<String, Integer>[] res = new Map[frequencies.length];

        for (int i = 0; i < frequencies.length; i++)
            if (handledIndices.contains(i))
                res[i] = transformFrequenciesToEncodingValues(frequencies[i]);

        return res;
    }

    /**
     * Calculates the encoding values values by frequencies keeping in the given dataset.
     *
     * @param dataset The dataset of frequencies for each feature aggregated in each partition.
     * @return Encoding values for each feature.
     */
    private Map<String, Integer> calculateEncodingValuesForLabelsByFrequencies(
        Dataset<EmptyContext, EncoderPartitionData> dataset) {
        Map<String, Integer> frequencies = calculateFrequenciesForLabels(dataset);

        Map<String, Integer> res = transformFrequenciesToEncodingValues(frequencies);

        return res;
    }

    /**
     * Transforms frequencies to the encoding values.
     *
     * @param frequencies Frequencies of categories for the specific feature.
     * @return Encoding values.
     */
    private Map<String, Integer> transformFrequenciesToEncodingValues(Map<String, Integer> frequencies) {
        Comparator<Map.Entry<String, Integer>> comp;

        comp = encoderSortingStgy == EncoderSortingStrategy.FREQUENCY_DESC ? Map.Entry.comparingByValue() : Collections.reverseOrder(Map.Entry.comparingByValue());

        final HashMap<String, Integer> resMap = frequencies.entrySet()
            .stream()
            .sorted(comp)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                (oldValue, newValue) -> oldValue, LinkedHashMap::new));

        int amountOfLabels = frequencies.size();

        for (Map.Entry<String, Integer> m : resMap.entrySet())
            m.setValue(--amountOfLabels);

        return resMap;
    }

    /**
     * Updates frequencies by values and features.
     *
     * @param row Feature vector.
     * @param categoryFrequencies Holds the frequencies of categories by values and features.
     * @return Updated frequencies by values and features.
     */
    private Map<String, Integer>[] updateFeatureFrequenciesForNextRow(LabeledVector row,
        Map<String, Integer>[] categoryFrequencies) {
        if (categoryFrequencies == null)
            categoryFrequencies = initializeCategoryFrequencies(row);
        else
            assert categoryFrequencies.length == row.size() : "Base preprocessor must return exactly "
                + categoryFrequencies.length + " features";

        for (int i = 0; i < categoryFrequencies.length; i++) {
            if (handledIndices.contains(i)) {
                String strVal;
                Object featureVal = row.features().getRaw(i);

                if (featureVal.equals(Double.NaN)) {
                    strVal = EncoderPreprocessor.KEY_FOR_NULL_VALUES;
                    row.features().setRaw(i, strVal);
                }
                else if (featureVal instanceof String)
                    strVal = (String)featureVal;
                else if (featureVal instanceof Double)
                    strVal = String.valueOf(featureVal);
                else
                    throw new RuntimeException("The type " + featureVal.getClass() + " is not supported for the feature values.");

                Map<String, Integer> map = categoryFrequencies[i];

                if (map.containsKey(strVal))
                    map.put(strVal, (map.get(strVal)) + 1);
                else
                    map.put(strVal, 1);
            }
        }
        return categoryFrequencies;
    }

    /**
     * Updates frequencies by values and features.
     *
     * @param row Feature vector.
     * @param labelFrequencies Holds the frequencies of categories by values and features.
     * @return Updated frequencies by values and features.
     */
    private Map<String, Integer> updateLabelFrequenciesForNextRow(LabeledVector row,
        Map<String, Integer> labelFrequencies) {
        if (labelFrequencies == null)
            labelFrequencies = new HashMap<>();

        String strVal;
        Object lbVal = row.label();

        if (lbVal.equals(Double.NaN) || lbVal == null)
            throw new UndefinedLabelException(row);

        else if (lbVal instanceof String)
            strVal = (String)lbVal;
        else if (lbVal instanceof Double)
            strVal = String.valueOf(lbVal);
        else
            throw new RuntimeException("The type " + lbVal.getClass() + " is not supported for the feature values.");

        if (labelFrequencies.containsKey(strVal))
            labelFrequencies.put(strVal, (labelFrequencies.get(strVal)) + 1);
        else
            labelFrequencies.put(strVal, 1);

        return labelFrequencies;
    }

    /**
     * Initialize frequencies for handled indices only.
     *
     * @param row Feature vector.
     * @return The array contains not null values for handled indices.
     */
    @NotNull private Map<String, Integer>[] initializeCategoryFrequencies(LabeledVector row) {
        Map<String, Integer>[] categoryFrequencies = new Map[row.size()];

        for (int i = 0; i < categoryFrequencies.length; i++)
            if (handledIndices.contains(i))
                categoryFrequencies[i] = new HashMap<>();

        return categoryFrequencies;
    }

    /**
     * Add the index of encoded feature.
     *
     * @param idx The index of encoded feature.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> withEncodedFeature(int idx) {
        handledIndices.add(idx);
        return this;
    }

    /**
     * Sets the encoder indexing strategy.
     *
     * @param encoderSortingStgy The encoder indexing strategy.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> withEncoderIndexingStrategy(EncoderSortingStrategy encoderSortingStgy) {
        this.encoderSortingStgy = encoderSortingStgy;
        return this;
    }

    /**
     * Sets the encoder preprocessor type.
     *
     * @param type The encoder preprocessor type.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> withEncoderType(EncoderType type) {
        this.encoderType = type;
        return this;
    }

    /**
     * Sets the indices of features which should be encoded.
     *
     * @param handledIndices Indices of features which should be encoded.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> withEncodedFeatures(Set<Integer> handledIndices) {
        this.handledIndices = handledIndices;
        return this;
    }
}
