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

package org.apache.ignite.ml.dataset.feature.extractor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

/**
 * Class for extracting labeled vectors from upstream. This is an abstract class providing API for
 * extracting feature and label values by "coordinates" of them from upstream objects. For example
 * {@link BinaryObject} can be upstream object and coordinates for them are names of fields with double-values.
 *
 * @param <K> Type of keys in upstream.
 * @param <V> Type of values in upstream.
 * @param <C> Type of "coordinate" - index of feature value in upstream object.
 * @param <L> Type of label for resulting vectors.
 */
public abstract class Vectorizer<K, V, C, L> implements FeatureLabelExtractor<K, V, L>, Serializable {
    /** Serial version uid. */
    private static final long serialVersionUID = 4301406952131379459L;
    /** If useAllValues == true then Vectorizer extract all fields as features from upstream object (except label). */
    private final boolean useAllValues;

    /** Extraction coordinates. */
    private List<C> extractionCoordinates;

    /** Label coordinate. */
    private C labelCoord;

    /** Excluded coordinates. */
    private HashSet<C> excludedCoords = new HashSet<>();
    /**
     * Creates an instance of Vectorizer.
     *
     * @param coords Coordinates for feature extraction. If array is empty then Vectorizer will extract all fields from
     * upstream object.
     */
    public Vectorizer(C... coords) {
        extractionCoordinates = Arrays.asList(coords);
        this.useAllValues = coords.length == 0;
    }

    /**
     * Extracts labeled vector from upstream object.
     *
     * @param key Key.
     * @param value Value.
     * @return vector.
     */
    public LabeledVector<L> apply(K key, V value) {
        L lbl = labelCoord != null ? label(labelCoord, key, value) : zero();

        List<C> allCoords = null;
        if (useAllValues) {
            allCoords = allCoords(key, value).stream()
                .filter(coord -> !coord.equals(labelCoord) && !excludedCoords.contains(coord))
                .collect(Collectors.toList());
        }

        int vectorLength = useAllValues ? allCoords.size() : extractionCoordinates.size();
        A.ensure(vectorLength > 0, "vectorLength > 0");

        double[] features = new double[vectorLength];
        List<C> coordinatesForExtraction = useAllValues ? allCoords : extractionCoordinates;
        for (int i = 0; i < coordinatesForExtraction.size(); i++)
            features[i] = feature(coordinatesForExtraction.get(i), key, value);
        return new LabeledVector<>(VectorUtils.of(features), lbl);
    }

    /**
     * Sets label coordinate for Vectorizer. By default it equals null and zero() will be used
     * as label value.
     *
     * @param labelCoord Label coordinate.
     * @return this.
     */
    public Vectorizer<K, V, C, L> labeled(C labelCoord) {
        this.labelCoord = labelCoord;
        return this;
    }

    /**
     * Exclude these coordinates from result vector.
     *
     * @param coords Coordinates.
     * @return this.
     */
    public Vectorizer<K,V,C,L> exclude(C ... coords) {
        this.excludedCoords.addAll(Arrays.asList(coords));
        return this;
    }

    /** {@inheritDoc} */
    @Override public LabeledVector<L> extract(K k, V v) {
        return apply(k, v);
    }

    /**
     * Extracts feature value by given coordinate.
     *
     * @param coord Coordinate.
     * @param key Key.
     * @param value Value.
     * @return feature value.
     */
    protected abstract Double feature(C coord, K key, V value);

    /**
     * Extract label value by given coordinate.
     *
     * @param coord Coordinate.
     * @param key Key.
     * @param value Value.
     * @return label value.
     */
    protected abstract L label(C coord, K key, V value);

    /**
     * Returns default label value for unlabeled data.
     *
     * @return label value.
     */
    protected abstract L zero();

    /**
     * Returns list of all coordinate with feature values.
     *
     * @param key Key.
     * @param value Value.
     * @return all coordinates list.
     */
    protected abstract List<C> allCoords(K key, V value);
}
