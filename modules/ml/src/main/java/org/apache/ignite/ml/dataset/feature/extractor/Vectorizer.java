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

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Class for extracting labeled vectors from upstream. This is an abstract class providing API for extracting feature
 * and label values by "coordinates" of them from upstream objects. For example {@link BinaryObject} can be upstream
 * object and coordinates for them are names of fields with double-values.
 *
 * @param <K> Type of keys in upstream.
 * @param <V> Type of values in upstream.
 * @param <C> Type of "coordinate" - index of feature value in upstream object.
 * @param <L> Type of label for resulting vectors.
 */
public abstract class Vectorizer<K, V, C extends Serializable, L> implements FeatureLabelExtractor<K, V, L>, Serializable {
    /** Label coordinate shortcut. */
    private LabelCoordinate lbCoordinateShortcut = null;

    /** Serial version uid. */
    private static final long serialVersionUID = 4301406952131379459L;

    /** If useAllValues == true then Vectorizer extract all fields as features from upstream object (except label). */
    private final boolean useAllValues;

    /** Extraction coordinates. */
    private List<C> extractionCoordinates;

    /** Label coordinate. */
    private C labelCoord;

    /**
     * Extracts labeled vector from upstream object.
     *
     * @param key Key.
     * @param value Value.
     * @return vector.
     */
    public LabeledVector<L> apply(K key, V value) {
        L lbl = isLabeled() ? label(labelCoord(key, value), key, value) : zero();

        List<C> allCoords = null;
        if (useAllValues) {
            allCoords = allCoords(key, value).stream()
                .filter(coord -> !coord.equals(labelCoord) && !excludedCoords.contains(coord))
                .collect(Collectors.toList());
        }

        int vectorLength = useAllValues ? allCoords.size() : extractionCoordinates.size();
        A.ensure(vectorLength >= 0, "vectorLength >= 0");

        List<C> coordinatesForExtraction = useAllValues ? allCoords : extractionCoordinates;
        Vector vector = createVector(vectorLength);
        for (int i = 0; i < coordinatesForExtraction.size(); i++) {
            Double feature = feature(coordinatesForExtraction.get(i), key, value);
            if (feature != null)
                vector.set(i, feature);
        }
        return new LabeledVector<>(vector, lbl);
    }

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
     * @return true if label in vector is valid.
     */
    private boolean isLabeled() {
        return labelCoord != null || lbCoordinateShortcut != null;
    }

    /**
     * Evaluates label coordinate if need.
     *
     * @param key Key.
     * @param value Value.
     * @return label coordinate.
     */
    private C labelCoord(K key, V value) {
        A.ensure(isLabeled(), "isLabeled");
        if (labelCoord != null)
            return labelCoord;
        else {
            List<C> allCoords = allCoords(key, value);
            A.ensure(!allCoords.isEmpty(), "!allCoords.isEmpty()");

            switch (lbCoordinateShortcut) {
                case FIRST:
                    labelCoord = allCoords.get(0);
                    break;
                case LAST:
                    labelCoord = allCoords.get(allCoords.size() - 1);
                    break;
                default:
                    throw new IllegalArgumentException();
            }

            return labelCoord;
        }
    }

    /**
     * Sets label coordinate for Vectorizer. By default it equals null and zero() will be used as label value.
     *
     * @param labelCoord Label coordinate.
     * @return this.
     */
    public Vectorizer<K, V, C, L> labeled(C labelCoord) {
        this.labelCoord = labelCoord;
        this.lbCoordinateShortcut = null;
        return this;
    }

    /**
     * Sets label coordinate for Vectorizer. By default it equals null and zero() will be used as label value.
     *
     * @param labelCoord Label coordinate.
     * @return this.
     */
    public Vectorizer<K, V, C, L> labeled(LabelCoordinate labelCoord) {
        this.lbCoordinateShortcut = labelCoord;
        this.labelCoord = null;
        return this;
    }

    /**
     * Exclude these coordinates from result vector.
     *
     * @param coords Coordinates.
     * @return this.
     */
    public Vectorizer<K, V, C, L> exclude(C... coords) {
        this.excludedCoords.addAll(Arrays.asList(coords));
        return this;
    }

    /**
     * Map vectorizer answer. This method should be called after creating basic vectorizer.
     * NOTE: function "func" should be on ignite servers.
     *
     * @param func mapper.
     * @param <L1> Type of new label.
     * @return mapped vectorizer.
     */
    public <L1> Vectorizer<K, V, C, L1> map(IgniteFunction<LabeledVector<L>, LabeledVector<L1>> func) {
        return new MappedVectorizer<>(this, func);
    }

    /**
     * Shotrcuts for coordinates in feature vector.
     */
    public enum LabelCoordinate {
        /** First. */FIRST,
        /** Last. */LAST
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

    /**
     * Create an instance of vector.
     *
     * @param size Vector size.
     * @return vector.
     */
    protected Vector createVector(int size) {
        return new DenseVector(size);
    }

    /**
     * @param <K> Type of key.
     * @param <V> Type of value.
     * @param <C> Type of coordinates.
     * @param <L0> Type of original label.
     * @param <L1> Type of mapped label.
     */
    private static class MappedVectorizer<K, V, C extends Serializable, L0, L1> extends VectorizerAdapter<K, V, C, L1> {
        /** Original vectorizer. */
        protected final Vectorizer<K, V, C, L0> original;

        /** Vectors mapping. */
        private final IgniteFunction<LabeledVector<L0>, LabeledVector<L1>> mapping;

        /**
         * Creates an instance of MappedVectorizer.
         */
        public MappedVectorizer(Vectorizer<K, V, C, L0> original,
            IgniteFunction<LabeledVector<L0>, LabeledVector<L1>> andThen) {

            this.original = original;
            this.mapping = andThen;
        }

        /** {@inheritDoc} */
        @Override public LabeledVector<L1> apply(K key, V value) {
            LabeledVector<L0> origVec = original.apply(key, value);
            return mapping.apply(origVec);
        }
    }

    /**
     * Utility class for convenient overridings.
     *
     * @param <K> Type of key.
     * @param <V> Type of value.
     * @param <C> Type of coordinate.
     * @param <L> Type od label.
     */
    public static class VectorizerAdapter<K, V, C extends Serializable, L> extends Vectorizer<K, V, C, L> {
        /** {@inheritDoc} */
        @Override protected Double feature(C coord, K key, V value) {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override protected L label(C coord, K key, V value) {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override protected L zero() {
            throw new IllegalStateException();
        }

        /** {@inheritDoc} */
        @Override protected List<C> allCoords(K key, V value) {
            throw new IllegalStateException();
        }
    }
}
