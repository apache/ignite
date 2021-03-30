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

package org.apache.ignite.internal.metastorage.watch;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Filter for listen key's changes on metastore.
 */
public interface KeyCriterion {
    /**
     * Translates any type of key criterion to range of keys.
     *
     * @return Ignite tuple with first key as start of range and second as the end.
     */
    public IgniteBiTuple<ByteArray, ByteArray> toRange();

    /**
     * Check if this key criterion contains the key.
     *
     * @return true if criterion contains the key, false otherwise.
     */
    public boolean contains(ByteArray key);

    /**
     * Simple criterion which contains exactly one key.
     */
    static class ExactCriterion implements KeyCriterion {
        /** The key of criterion. */
        private final ByteArray key;

        /**
         * Creates the instance of exact criterion.
         *
         * @param key Instance of the reference key.
         */
        public ExactCriterion(ByteArray key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<ByteArray, ByteArray> toRange() {
            return new IgniteBiTuple<>(key, key);
        }

        /** {@inheritDoc} */
        @Override public boolean contains(ByteArray key) {
            return this.key.equals(key);
        }

    }

    /**
     * Criterion which contains the range of keys.
     */
    static class RangeCriterion implements KeyCriterion {
        /** Start of the range. */
        private final ByteArray from;

        /** End of the range (exclusive). */
        private final ByteArray to;

        /**
         * Creates the instance of range criterion.
         *
         * @param from Start of the range.
         * @param to End of the range (exclusive).
         */
        public RangeCriterion(ByteArray from, ByteArray to) {
            this.from = from;
            this.to = to;
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<ByteArray, ByteArray> toRange() {
            return new IgniteBiTuple<>(from, to);
        }

        /** {@inheritDoc} */
        @Override public boolean contains(ByteArray key) {
            return key.compareTo(from) >= 0 && key.compareTo(to) < 0;
        }
    }

    /**
     * Criterion which consists collection of keys.
     */
    static class CollectionCriterion implements KeyCriterion {
        /** Collection of keys. */
        private final Collection<ByteArray> keys;

        /**
         * Creates the instance of collection criterion.
         *
         * @param keys Collection of keys.
         */
        public CollectionCriterion(Collection<ByteArray> keys) {
            this.keys = keys;
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<ByteArray, ByteArray> toRange() {
            return new IgniteBiTuple<>(Collections.min(keys), Collections.max(keys));
        }

        /** {@inheritDoc} */
        @Override public boolean contains(ByteArray key) {
            return keys.contains(key);
        }
    }

    /**
     * Criterion which consists of all keys with defined prefix.
     */
    static class PrefixCriterion implements KeyCriterion {
        /** Prefix of the key. */
        private final ByteArray prefixKey;

        /**
         * Creates the instance of prefix key criterion.
         *
         * @param prefixKey Prefix of the key.
         */
        public PrefixCriterion(ByteArray prefixKey) {
            this.prefixKey = prefixKey;
        }

        /** {@inheritDoc} */
        @Override public IgniteBiTuple<ByteArray, ByteArray> toRange() {
            var bytes = Arrays.copyOf(prefixKey.bytes(), prefixKey.bytes().length);

            if (bytes[bytes.length - 1] != Byte.MAX_VALUE)
                bytes[bytes.length - 1]++;
            else
                bytes = Arrays.copyOf(bytes, bytes.length + 1);

            return new IgniteBiTuple<>(prefixKey, new ByteArray(bytes));
        }

        /** {@inheritDoc} */
        @Override public boolean contains(ByteArray key) {
            return key.compareTo(prefixKey) >= 0 && key.compareTo(toRange().getValue()) < 0;
        }
    }
}
