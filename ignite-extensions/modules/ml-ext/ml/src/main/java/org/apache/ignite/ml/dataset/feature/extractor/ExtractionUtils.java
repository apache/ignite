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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Class aggregates helper classes and shortcut-classes with default types and behaviour for Vectorizers.
 */
public class ExtractionUtils {
    /**
     * Vectorizer with double-label containing on same level as feature values.
     *
     * @param <K> Key type.
     * @param <V> Value type
     * @param <C> Type of coordinate.
     */
    public abstract static class DefaultLabelVectorizer<K, V, C extends Serializable> extends Vectorizer<K, V, C, Double> {
        /** Serial version uid. */
        private static final long serialVersionUID = 2876703640636013770L;

        /**
         * Creates an instance of Vectorizer.
         *
         * @param coords Coordinates.
         */
        public DefaultLabelVectorizer(C... coords) {
            super(coords);
        }

        /** {@inheritDoc} */
        @Override protected Double label(C coord, K key, V value) {
            return (Double)feature(coord, key, value);
        }

        /** {@inheritDoc} */
        @Override protected Double zero() {
            return 0.0;
        }
    }

    /**
     * Vectorizer with String-label containing on same level as feature values.
     *
     * @param <K> Key type.
     * @param <V> Value type
     * @param <C> Type of coordinate.
     */
    public abstract static class ObjectLabelVectorizer<K, V, C extends Serializable> extends Vectorizer<K, V, C, Object> {
        /** Serial version uid. */
        private static final long serialVersionUID = 2226703640636013770L;

        /**
         * Creates an instance of Vectorizer.
         *
         * @param coords Coordinates.
         */
        public ObjectLabelVectorizer(C... coords) {
            super(coords);
        }

        /** {@inheritDoc} */
        @Override protected Object label(C coord, K key, V value) {
            return feature(coord, key, value);
        }

        /** {@inheritDoc} */
        @Override protected Object zero() {
            return "default";
        }
    }

    /**
     * Vectorizer with integer coordinates.
     *
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public abstract static class IntCoordObjectLabelVectorizer<K, V> extends ObjectLabelVectorizer<K, V, Integer> {
        /** Serial version uid. */
        private static final long serialVersionUID = -2834141133396507699L;

        /**
         * Creates an instance of Vectorizer.
         *
         * @param coords Coordinates.
         */
        public IntCoordObjectLabelVectorizer(Integer... coords) {
            super(coords);
        }
    }

    /**
     * Vectorizer extracting vectors from array-like structure with finite size and integer coordinates.
     *
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public abstract static class ArrayLikeObjectLabelVectorizer<K, V> extends IntCoordObjectLabelVectorizer<K, V> {
        /** Serial version uid. */
        private static final long serialVersionUID = 5383770258177533358L;

        /**
         * Creates an instance of Vectorizer.
         *
         * @param coords Coordinates.
         */
        public ArrayLikeObjectLabelVectorizer(Integer... coords) {
            super(coords);
        }

        /**
         * Size of array-like structure of upstream object.
         *
         * @param key Key.
         * @param value Value.
         * @return size.
         */
        protected abstract int sizeOf(K key, V value);

        /** {@inheritDoc} */
        @Override protected List<Integer> allCoords(K key, V value) {
            return IntStream.range(0, sizeOf(key, value)).boxed().collect(Collectors.toList());
        }
    }

    /**
     * Vectorizer with String-coordinates.
     *
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public abstract static class StringCoordVectorizer<K, V> extends DefaultLabelVectorizer<K, V, String> {
        /** Serial version uid. */
        private static final long serialVersionUID = 6989473570977667636L;

        /**
         * Creates an instance of Vectorizer.
         *
         * @param coords Coordinates.
         */
        public StringCoordVectorizer(String... coords) {
            super(coords);
        }
    }

    /**
     * Vectorizer with integer coordinates.
     *
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public abstract static class IntCoordVectorizer<K, V> extends DefaultLabelVectorizer<K, V, Integer> {
        /** Serial version uid. */
        private static final long serialVersionUID = -1734141133396507699L;

        /**
         * Creates an instance of Vectorizer.
         *
         * @param coords Coordinates.
         */
        public IntCoordVectorizer(Integer... coords) {
            super(coords);
        }
    }

    /**
     * Vectorizer extracting vectors from array-like structure with finite size and integer coordinates.
     *
     * @param <K> Type of key.
     * @param <V> Type of value.
     */
    public abstract static class ArrayLikeVectorizer<K, V> extends IntCoordVectorizer<K, V> {
        /** Serial version uid. */
        private static final long serialVersionUID = 5383770258177577358L;

        /**
         * Creates an instance of Vectorizer.
         *
         * @param coords Coordinates.
         */
        public ArrayLikeVectorizer(Integer... coords) {
            super(coords);
        }

        /**
         * Size of array-like structure of upstream object.
         *
         * @param key Key.
         * @param value Value.
         * @return size.
         */
        protected abstract int sizeOf(K key, V value);

        /** {@inheritDoc} */
        @Override protected List<Integer> allCoords(K key, V value) {
            return IntStream.range(0, sizeOf(key, value)).boxed().collect(Collectors.toList());
        }
    }
}
