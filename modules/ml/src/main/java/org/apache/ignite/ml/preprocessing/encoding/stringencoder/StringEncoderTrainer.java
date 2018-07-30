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

package org.apache.ignite.ml.preprocessing.encoding.stringencoder;

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
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.PreprocessingTrainer;
import org.jetbrains.annotations.NotNull;

/**
 * Trainer of the String Encoder preprocessor.
 * The String Encoder encodes string values (categories) to double values in range [0.0, amountOfCategories)
 * where the most popular value will be presented as 0.0 and the least popular value presented with amountOfCategories-1 value.
 *
 * @param <K> Type of a key in {@code upstream} data.
 * @param <V> Type of a value in {@code upstream} data.
 */
public class StringEncoderTrainer<K, V> implements PreprocessingTrainer<K, V, Object[], Vector> {
    /** Indices of features which should be encoded. */
    private Set<Integer> handledIndices = new HashSet<>();

    /** {@inheritDoc} */
    @Override public StringEncoderPreprocessor<K, V> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Object[]> basePreprocessor) {
        if(handledIndices.isEmpty())
            throw new RuntimeException("Add indices of handled features");

        try (Dataset<EmptyContext, StringEncoderPartitionData> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new EmptyContext(),
            (upstream, upstreamSize, ctx) -> {
                // This array will contain not null values for handled indices
                Map<String, Integer>[] categoryFrequencies = null;

                while (upstream.hasNext()) {
                    UpstreamEntry<K, V> entity = upstream.next();
                    Object[] row = basePreprocessor.apply(entity.getKey(), entity.getValue());
                    categoryFrequencies = calculateFrequencies(row, categoryFrequencies);
                }
                return new StringEncoderPartitionData()
                    .withCategoryFrequencies(categoryFrequencies);
            }
        )) {
            Map<String, Integer>[] encodingValues = calculateEncodingValuesByFrequencies(dataset);

            return new StringEncoderPreprocessor<>(encodingValues, basePreprocessor, handledIndices);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Calculates the encoding values values by frequencies keeping in the given dataset.
     *
     * @param dataset The dataset of frequencies for each feature aggregated in each partition.
     * @return Encoding values for each feature.
     */
    private Map<String, Integer>[] calculateEncodingValuesByFrequencies(
        Dataset<EmptyContext, StringEncoderPartitionData> dataset) {
        Map<String, Integer>[] frequencies = dataset.compute(
            StringEncoderPartitionData::categoryFrequencies,
            (a, b) -> {
                if (a == null)
                    return b;

                if (b == null)
                    return a;

                assert a.length == b.length;

                for (int i = 0; i < a.length; i++) {
                    if(handledIndices.contains(i)){
                        int finalI = i;
                        a[i].forEach((k, v) -> b[finalI].merge(k, v, (f1, f2) -> f1 + f2));
                    }
                }
                return b;
            }
        );

        Map<String, Integer>[] res = new HashMap[frequencies.length];

        for (int i = 0; i < frequencies.length; i++)
            if(handledIndices.contains(i))
                res[i] = transformFrequenciesToEncodingValues(frequencies[i]);

        return res;
    }

    /**
     * Transforms frequencies to the encoding values.
     *
     * @param frequencies Frequencies of categories for the specific feature.
     * @return Encoding values.
     */
    private Map<String, Integer> transformFrequenciesToEncodingValues(Map<String, Integer> frequencies) {
        final HashMap<String, Integer> resMap = frequencies.entrySet()
            .stream()
            .sorted(Map.Entry.comparingByValue())
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
    private Map<String, Integer>[] calculateFrequencies(Object[] row, Map<String, Integer>[] categoryFrequencies) {
        if (categoryFrequencies == null)
            categoryFrequencies = initializeCategoryFrequencies(row);
        else
            assert categoryFrequencies.length == row.length : "Base preprocessor must return exactly "
                + categoryFrequencies.length + " features";

        for (int i = 0; i < categoryFrequencies.length; i++) {
            if(handledIndices.contains(i)){
                String strVal;
                Object featureVal = row[i];

                if(featureVal.equals(Double.NaN)) {
                    strVal = "";
                    row[i] = strVal;
                }
                else strVal = (String)featureVal;

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
     * Initialize frequencies for handled indices only.
     * @param row Feature vector.
     * @return The array contains not null values for handled indices.
     */
    @NotNull private Map<String, Integer>[] initializeCategoryFrequencies(Object[] row) {
        Map<String, Integer>[] categoryFrequencies = new HashMap[row.length];

        for (int i = 0; i < categoryFrequencies.length; i++)
            if(handledIndices.contains(i))
                categoryFrequencies[i] = new HashMap<>();

        return categoryFrequencies;
    }

    /**
     * Add the index of encoded feature.
     * @param idx The index of encoded feature.
     * @return The changed trainer.
     */
    public StringEncoderTrainer<K, V> encodeFeature(int idx){
        handledIndices.add(idx);
        return this;
    }
}
