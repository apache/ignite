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
import org.apache.ignite.ml.preprocessing.encoding.target.TargetCounter;
import org.apache.ignite.ml.preprocessing.encoding.target.TargetEncoderPreprocessor;
import org.apache.ignite.ml.preprocessing.encoding.target.TargetEncodingMeta;
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

    /** Index of target for target encoding */
    private Integer targetLabelIndex;

    /** Smoting param for target encoding */
    private Double smoothing = 1d;

    /** Min samples leaf for target encoding */
    private Integer minSamplesLeaf = 1;

    /** Min category size for target concoding */
    private Long minCategorySize = 10L;

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
                else if (encoderType == EncoderType.TARGET_ENCODER) {
                    TargetCounter[] targetCounter = null;

                    while (upstream.hasNext()) {
                        UpstreamEntry<K, V> entity = upstream.next();
                        LabeledVector<Double> row = basePreprocessor.apply(entity.getKey(), entity.getValue());

                        targetCounter = updateTargetCountersForNextRow(row, targetCounter);
                    }
                    partData.withTargetCounters(targetCounter);
                } else {
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
                case TARGET_ENCODER:
                    return new TargetEncoderPreprocessor<>(calculateTargetEncodingFrequencies(dataset), basePreprocessor, handledIndices);
                default:
                    throw new IllegalStateException("Define the type of the resulting prerocessor.");
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculates encoding frequencies as avarage category target on amount of rows in dataset.
     *
     * NOTE: The amount of rows is calculated as sum of absolute frequencies.
     *
     * @param dataset Dataset.
     * @return Encoding frequency for each feature.
     */
    private TargetEncodingMeta[] calculateTargetEncodingFrequencies(Dataset<EmptyContext, EncoderPartitionData> dataset) {
        TargetCounter[] targetCounters = dataset.compute(
            EncoderPartitionData::targetCounters,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++) {
                    if (handledIndices.contains(i)) {
                        int finalI = i;
                        b[i].setTargetSum(a[i].getTargetSum() + b[i].getTargetSum());
                        b[i].setTargetCount(a[i].getTargetCount() + b[i].getTargetCount());
                        a[i].getCategoryCounts()
                            .forEach((k, v) -> b[finalI].getCategoryCounts().merge(k, v, Long::sum));
                        a[i].getCategoryTargetSum()
                            .forEach((k, v) -> b[finalI].getCategoryTargetSum().merge(k, v, Double::sum));
                    }
                }
                return b;
            }
        );

        TargetEncodingMeta[] targetEncodingMetas = new TargetEncodingMeta[targetCounters.length];
        for (int i = 0; i < targetCounters.length; i++) {
            if (handledIndices.contains(i)) {
                TargetCounter targetCounter = targetCounters[i];

                targetEncodingMetas[i] = new TargetEncodingMeta()
                    .withGlobalMean(targetCounter.getTargetSum() / targetCounter.getTargetCount())
                    .withCategoryMean(calculateCategoryTargetEncodingFrequency(targetCounter));
            }
        }
        return targetEncodingMetas;
    }

    /**
     * Calculates encoding frequencies as avarage category target on amount of rows in dataset.
     *
     * @param targetCounter target Counter.
     * @return Encoding frequency for each category.
     */
    private Map<String, Double> calculateCategoryTargetEncodingFrequency(TargetCounter targetCounter) {
        double prior = targetCounter.getTargetSum() /
            targetCounter.getTargetCount();

        return targetCounter.getCategoryTargetSum().entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                value -> {
                    double targetSum = targetCounter.getCategoryTargetSum()
                        .get(value.getKey());
                    long categorySize = targetCounter.getCategoryCounts()
                        .get(value.getKey());

                    if (categorySize < minCategorySize) {
                        return prior;
                    } else {
                        double categoryMean = targetSum / categorySize;

                        double smoove = 1 / (1 +
                            Math.exp(-(categorySize - minSamplesLeaf) / smoothing));
                        return prior * (1 - smoove) + categoryMean * smoove;
                    }
                }
            ));
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
     * Updates frequencies by values and features.
     *
     * @param row Feature vector.
     * @param targetCounters Holds the frequencies of categories by values and features.
     * @return target counter.
     */
    private TargetCounter[] updateTargetCountersForNextRow(LabeledVector row,
                                                           TargetCounter[] targetCounters) {
        if (targetCounters == null)
            targetCounters = initializeTargetCounters(row);
        else
            assert targetCounters.length == row.size() : "Base preprocessor must return exactly "
                + targetCounters.length + " features";

        double targetValue = row.features().get(targetLabelIndex);

        for (int i = 0; i < targetCounters.length; i++) {
            if (handledIndices.contains(i)) {
                String strVal;
                Object featureVal = row.features().getRaw(i);

                if (featureVal.equals(Double.NaN)) {
                    strVal = EncoderPreprocessor.KEY_FOR_NULL_VALUES;
                    row.features().setRaw(i, strVal);
                }
                else if (featureVal instanceof String)
                    strVal = (String)featureVal;
                else if (featureVal instanceof Number)
                    strVal = String.valueOf(featureVal);
                else if (featureVal instanceof Boolean)
                    strVal = String.valueOf(featureVal);
                else
                    throw new RuntimeException("The type " + featureVal.getClass() + " is not supported for the feature values.");

                TargetCounter targetCounter = targetCounters[i];
                targetCounter.setTargetCount(targetCounter.getTargetCount() + 1);
                targetCounter.setTargetSum(targetCounter.getTargetSum() + targetValue);

                Map<String, Long> categoryCounts = targetCounter.getCategoryCounts();

                if (categoryCounts.containsKey(strVal)) {
                    categoryCounts.put(strVal, categoryCounts.get(strVal) + 1);
                } else {
                    categoryCounts.put(strVal, 1L);
                }

                Map<String, Double> categoryTargetSum = targetCounter.getCategoryTargetSum();
                if (categoryTargetSum.containsKey(strVal)) {
                    categoryTargetSum.put(strVal, categoryTargetSum.get(strVal) + targetValue);
                } else {
                    categoryTargetSum.put(strVal, targetValue);
                }
            }
        }
        return targetCounters;
    }

    /**
     * Initialize target counters for handled indices only.
     *
     * @param row Features vector.
     * @return target counter.
     */
    private TargetCounter[] initializeTargetCounters(LabeledVector row) {
        TargetCounter[] targetCounter = new TargetCounter[row.size()];

        for (int i = 0; i < row.size(); i++) {
            if (handledIndices.contains(i)) {
                targetCounter[i] = new TargetCounter();
            }
        }

        return targetCounter;
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
        this.handledIndices.addAll(handledIndices);
        return this;
    }

    /**
     * Sets the target label index.
     * @param targetLabelIndex Index of target label.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> labeled(Integer targetLabelIndex) {
        this.targetLabelIndex = targetLabelIndex;
        return this;
    }

    /**
     * Sets the smoothing for target encoding.
     * @param smoothing smoothing value.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> smoothing(Double smoothing) {
        this.smoothing = smoothing;
        return this;
    }

    /**
     * Sets the minSamplesLeaf for target encoding.
     * @param minSamplesLeaf min samples leaf.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> minSamplesLeaf(Integer minSamplesLeaf) {
        this.minSamplesLeaf = minSamplesLeaf;
        return this;
    }

    /**
     * Sets the min category size for category target encoding.
     * Category less then minCategorySize will be encoded with avarage target value.
     * @param minCategorySize min samples leaf.
     * @return The changed trainer.
     */
    public EncoderTrainer<K, V> minCategorySize(Long minCategorySize) {
        this.minCategorySize = minCategorySize;
        return this;
    }
}
