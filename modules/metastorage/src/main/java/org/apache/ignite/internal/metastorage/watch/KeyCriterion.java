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
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.lang.ByteArray;

/**
 * Filter for listen key's changes on metastore.
 */
public abstract class KeyCriterion {
    /**
     * Checks if this key criterion contains the key.
     *
     * @return {@code true} if criterion contains the key, {@code false} otherwise.
     */
    public abstract boolean contains(ByteArray key);

    /**
     * Union current key criterion with another one.
     *
     * @param keyCriterion Criterion to calculate the union with.
     * @param swapTry Set to {@code true} if current criterion can't calculate the union
     *                and trying to calculate it from the opposite side.
     * @return Result key criterion.
     */
    protected abstract KeyCriterion union(KeyCriterion keyCriterion, boolean swapTry);

    /**
     * Union two key criteria and produce the new one.
     *
     * Rules for the union of different types of criteria:
     * <pre>
     * exact + exact = collection|exact
     * collection + exact = collection
     * collection + collection = collection
     * range + exact = range
     * range + collection = range
     * range + range = range
     * </pre>
     *
     * @param keyCriterion Criterion to calculate the union with.
     * @return Result of criteria union.
     */
    public KeyCriterion union(KeyCriterion keyCriterion) {
        return union(keyCriterion, false);
    }

    /**
     * Creates a common exception that indicates the given key criterions cannot be combined.
     *
     * @param keyCriterion1 Criterion.
     * @param keyCriterion2 Criterion.
     *
     * @return Common exception that indicates the given key criterions cannot be combined.
     */
    private static RuntimeException unsupportedUnionException(KeyCriterion keyCriterion1, KeyCriterion keyCriterion2) {
        return new UnsupportedOperationException("Can't calculate the union between " + keyCriterion1.getClass() +
            "and " + keyCriterion2.getClass() + " key criteria.");
    }

    /**
     * Criterion which contains the range of keys.
     */
    public static class RangeCriterion extends KeyCriterion {
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
        @Override public boolean contains(ByteArray key) {
            return key.compareTo(from) >= 0 && key.compareTo(to) < 0;
        }

        /**
         * Calculates range representation for prefix criterion
         * as {@code (prefixKey, nextKey(prefixKey)) }.
         *
         * @param prefixKey Prefix criterion.
         * @return Calculated range
         */
        public static RangeCriterion fromPrefixKey(ByteArray prefixKey) {
            return new RangeCriterion(prefixKey, nextKey(prefixKey));
        }

        /** {@inheritDoc} */
        @Override protected KeyCriterion union(KeyCriterion keyCriterion, boolean swapTry) {
            ByteArray from;
            ByteArray to;

            if (keyCriterion instanceof ExactCriterion) {
                from = ((ExactCriterion)keyCriterion).key;
                to = nextKey(from);
            }
            else if (keyCriterion instanceof CollectionCriterion) {
                from = Collections.min(((CollectionCriterion)keyCriterion).keys);
                to = nextKey(Collections.max(((CollectionCriterion)keyCriterion).keys));
            }
            else if (keyCriterion instanceof RangeCriterion) {
                from = ((RangeCriterion)keyCriterion).from;
                to = ((RangeCriterion)keyCriterion).to;
            }
            else if (!swapTry)
                return keyCriterion.union(this, true);
            else
                throw KeyCriterion.unsupportedUnionException(this, keyCriterion);

            return new RangeCriterion(
                minFromNullables(this.from, from),
                maxFromNullables(this.to, to)
            );

        }

        /**
         * Calculates the maximum of two keys in the scope of keys' range.
         * According to the logic of range keys - null is an equivalent to +Inf in the range end position.
         *
         * @param key1 The first key to compare.
         * @param key2 The second key to compare.
         * @return Maximum key.
         */
        private static ByteArray maxFromNullables(ByteArray key1, ByteArray key2) {
            if (key1 != null && key2 != null)
               return (key1.compareTo(key2) >= 0) ? key1 : key2;
            else
                return null;
        }

        /**
         * Calculates the minimum of two keys in the scope of keys' range.
         * According to the logic of range keys - null is an equivalent to -Inf in the range start position.
         *
         * @param key1 The first key to compare.
         * @param key2 The second key to compare.
         * @return Minimum key.
         */
        private static ByteArray minFromNullables(ByteArray key1, ByteArray key2) {
            if (key1 != null && key2 != null)
                return (key1.compareTo(key2) < 0) ? key1 : key2;
            else
                return null;
        }

        /**
         * Calculates the next key for received key.
         *
         * @param key Input key.
         * @return Next key.
         */
        private static ByteArray nextKey(ByteArray key) {
            var bytes = Arrays.copyOf(key.bytes(), key.bytes().length);

            if (bytes[bytes.length - 1] != Byte.MAX_VALUE)
                bytes[bytes.length - 1]++;
            else
                bytes = Arrays.copyOf(bytes, bytes.length + 1);

            return new ByteArray(bytes);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            RangeCriterion criterion = (RangeCriterion)o;
            return Objects.equals(from, criterion.from) && Objects.equals(to, criterion.to);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(from, to);
        }

        /**
         * @return Start of the range.
         */
        public ByteArray from() {
            return from;
        }

        /**
         * @return End of the range (exclusive).
         */
        public ByteArray to() {
            return to;
        }
    }

    /**
     * Criterion which consists collection of keys.
     */
    public static class CollectionCriterion extends KeyCriterion {
        /** Collection of keys. */
        private final Set<ByteArray> keys;

        /**
         * Creates the instance of collection criterion.
         *
         * @param keys Collection of keys.
         */
        public CollectionCriterion(Collection<ByteArray> keys) {
            this.keys = new HashSet<>(keys);
        }

        /** {@inheritDoc} */
        @Override public boolean contains(ByteArray key) {
            return keys.contains(key);
        }

        /** {@inheritDoc} */
        @Override protected KeyCriterion union(KeyCriterion keyCriterion, boolean swapTry) {
            var newKeys = new HashSet<>(keys);

            if (keyCriterion instanceof ExactCriterion) {
                newKeys.add(((ExactCriterion)keyCriterion).key);

                return new CollectionCriterion(newKeys);
            }
            else if (keyCriterion instanceof CollectionCriterion) {
                newKeys.addAll(((CollectionCriterion)keyCriterion).keys);

                return new CollectionCriterion(newKeys);
            } else if (!swapTry)
                return keyCriterion.union(keyCriterion, true);
            else
                throw KeyCriterion.unsupportedUnionException(this, keyCriterion);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            CollectionCriterion criterion = (CollectionCriterion)o;
            return Objects.equals(keys, criterion.keys);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(keys);
        }

        /**
         * @return Collection of keys.
         */
        public Set<ByteArray> keys() {
            return keys;
        }
    }

    /**
     * Simple criterion which contains exactly one key.
     */
    public static class ExactCriterion extends KeyCriterion {
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
        @Override public boolean contains(ByteArray key) {
            return this.key.equals(key);
        }

        /** {@inheritDoc} */
        @Override protected KeyCriterion union(KeyCriterion keyCriterion, boolean swapTry) {
            if (keyCriterion instanceof ExactCriterion) {
                if (equals(keyCriterion))
                    return this;
                else
                    return new CollectionCriterion(Arrays.asList(key, ((ExactCriterion)keyCriterion).key));
            }
            else if (!swapTry)
                return keyCriterion.union(this, true);
            else
                throw KeyCriterion.unsupportedUnionException(this, keyCriterion);
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            ExactCriterion criterion = (ExactCriterion)o;
            return Objects.equals(key, criterion.key);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(key);
        }

        /**
         * @return The key of criterion.
         */
        public ByteArray key() {
            return key;
        }
    }
}
