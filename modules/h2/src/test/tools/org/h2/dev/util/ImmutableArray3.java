/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.util;

import java.util.Iterator;
import org.h2.mvstore.DataUtils;

/**
 * An immutable array.
 *
 * @param <K> the type
 */
public abstract class ImmutableArray3<K> implements Iterable<K> {

    private static final int MAX_LEVEL = 4;

    private static final ImmutableArray3<?> EMPTY = new Plain<>(new Object[0]);

    /**
     * Get the length.
     *
     * @return the length
     */
    public abstract int length();

    /**
     * Get the entry at this index.
     *
     * @param index the index
     * @return the entry
     */
    public abstract K get(int index);

    /**
     * Set the entry at this index.
     *
     * @param index the index
     * @param obj the object
     * @return the new immutable array
     */
    public abstract ImmutableArray3<K> set(int index, K obj);

    /**
     * Insert an entry at this index.
     *
     * @param index the index
     * @param obj the object
     * @return the new immutable array
     */
    public abstract ImmutableArray3<K> insert(int index, K obj);

    /**
     * Remove the entry at this index.
     *
     * @param index the index
     * @return the new immutable array
     */
    public abstract ImmutableArray3<K> remove(int index);

    /**
     * Get a sub-array.
     *
     * @param fromIndex the index of the first entry
     * @param toIndex the end index, plus one
     * @return the new immutable array
     */
    public ImmutableArray3<K> subArray(int fromIndex, int toIndex) {
        int len = toIndex - fromIndex;
        @SuppressWarnings("unchecked")
        K[] array = (K[]) new Object[len];
        for (int i = 0; i < len; i++) {
            array[i] = get(fromIndex + i);
        }
        return new Plain<>(array);
    }

    /**
     * Create an immutable array.
     *
     * @param array the data
     * @return the new immutable array
     */
    @SafeVarargs
    public static <K> ImmutableArray3<K> create(K... array) {
        return new Plain<>(array);
    }

    /**
     * Get the data.
     *
     * @return the data
     */
    public K[] array() {
        int len = length();
        @SuppressWarnings("unchecked")
        K[] array = (K[]) new Object[len];
        for (int i = 0; i < len; i++) {
            array[i] = get(i);
        }
        return array;
    }

    /**
     * Get the level of "abstraction".
     *
     * @return the level
     */
    abstract int level();

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        for (K obj : this) {
            buff.append(' ').append(obj);
        }
        return buff.toString();
    }

    /**
     * Get an empty immutable array.
     *
     * @param <K> the key type
     * @return the array
     */
    @SuppressWarnings("unchecked")
    public static <K> ImmutableArray3<K> empty() {
        return (ImmutableArray3<K>) EMPTY;
    }

    /**
     * Get an iterator over all entries.
     *
     * @return the iterator
     */
    @Override
    public Iterator<K> iterator() {
        return new Iterator<K>() {

            ImmutableArray3<K> a = ImmutableArray3.this;
            int index;

            @Override
            public boolean hasNext() {
                return index < a.length();
            }

            @Override
            public K next() {
                return a.get(index++);
            }

            @Override
            public void remove() {
                throw DataUtils.newUnsupportedOperationException("remove");
            }

        };
    }


    /**
     * An immutable array backed by an array.
     *
     * @param <K> the type
     */
    static class Plain<K> extends ImmutableArray3<K> {

        /**
         * The array.
         */
        private final K[] array;

        public Plain(K[] array) {
            this.array = array;
        }

        @Override
        public K get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public ImmutableArray3<K> set(int index, K obj) {
            return new Set<>(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> insert(int index, K obj) {
            return new Insert<>(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> remove(int index) {
            return new Remove<>(this, index);
        }

        /**
         * Get a plain array with the given entry updated.
         *
         * @param <K> the type
         * @param base the base type
         * @param index the index
         * @param obj the object
         * @return the immutable array
         */
        static <K> ImmutableArray3<K> set(ImmutableArray3<K> base, int index, K obj) {
            int len = base.length();
            @SuppressWarnings("unchecked")
            K[] array = (K[]) new Object[len];
            for (int i = 0; i < len; i++) {
                array[i] = i == index ? obj : base.get(i);
            }
            return new Plain<>(array);
        }

        /**
         * Get a plain array with the given entry inserted.
         *
         * @param <K> the type
         * @param base the base type
         * @param index the index
         * @param obj the object
         * @return the immutable array
         */
        static <K> ImmutableArray3<K> insert(ImmutableArray3<K> base, int index, K obj) {
            int len = base.length() + 1;
            @SuppressWarnings("unchecked")
            K[] array = (K[]) new Object[len];
            for (int i = 0; i < len; i++) {
                array[i] = i == index ? obj : i < index ? base.get(i) : base.get(i - 1);
            }
            return new Plain<>(array);
        }

        /**
         * Get a plain array with the given entry removed.
         *
         * @param <K> the type
         * @param base the base type
         * @param index the index
         * @return the immutable array
         */
        static <K> ImmutableArray3<K> remove(ImmutableArray3<K> base, int index) {
            int len = base.length() - 1;
            @SuppressWarnings("unchecked")
            K[] array = (K[]) new Object[len];
            for (int i = 0; i < len; i++) {
                array[i] = i < index ? base.get(i) : base.get(i + 1);
            }
            return new Plain<>(array);
        }

        @Override
        int level() {
            return 0;
        }

    }

    /**
     * An immutable array backed by another immutable array, with one element
     * changed.
     *
     * @param <K> the type
     */
    static class Set<K> extends ImmutableArray3<K> {

        private final int index;
        private final ImmutableArray3<K> base;
        private final K obj;

        Set(ImmutableArray3<K> base, int index, K obj) {
            this.base = base;
            this.index = index;
            this.obj = obj;
        }

        @Override
        public int length() {
            return base.length();
        }

        @Override
        public K get(int index) {
            return this.index == index ? obj : base.get(index);
        }

        @Override
        public ImmutableArray3<K> set(int index, K obj) {
            if (index == this.index) {
                return new Set<>(base, index, obj);
            } else if (level() < MAX_LEVEL) {
                return new Set<>(this, index, obj);
            }
            return Plain.set(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> insert(int index, K obj) {
            if (level() < MAX_LEVEL) {
                return new Insert<>(this, index, obj);
            }
            return Plain.insert(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> remove(int index) {
            if (level() < MAX_LEVEL) {
                return new Remove<>(this, index);
            }
            return Plain.remove(this, index);
        }

        @Override
        int level() {
            return base.level() + 1;
        }

    }

    /**
     * An immutable array backed by another immutable array, with one element
     * added.
     *
     * @param <K> the type
     */
    static class Insert<K> extends ImmutableArray3<K> {

        private final int index;
        private final ImmutableArray3<K> base;
        private final K obj;

        Insert(ImmutableArray3<K> base, int index, K obj) {
            this.base = base;
            this.index = index;
            this.obj = obj;
        }

        @Override
        public ImmutableArray3<K> set(int index, K obj) {
            if (level() < MAX_LEVEL) {
                return new Set<>(this, index, obj);
            }
            return Plain.set(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> insert(int index, K obj) {
            if (level() < MAX_LEVEL) {
                return new Insert<>(this, index, obj);
            }
            return Plain.insert(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> remove(int index) {
            if (index == this.index) {
                return base;
            } else if (level() < MAX_LEVEL) {
                return new Remove<>(this, index);
            }
            return Plain.remove(this, index);
        }

        @Override
        public int length() {
            return base.length() + 1;
        }

        @Override
        public K get(int index) {
            if (index == this.index) {
                return obj;
            } else if (index < this.index) {
                return base.get(index);
            }
            return base.get(index - 1);
        }

        @Override
        int level() {
            return base.level() + 1;
        }

    }

    /**
     * An immutable array backed by another immutable array, with one element
     * removed.
     *
     * @param <K> the type
     */
    static class Remove<K> extends ImmutableArray3<K> {

        private final int index;
        private final ImmutableArray3<K> base;

        Remove(ImmutableArray3<K> base, int index) {
            this.base = base;
            this.index = index;
        }

        @Override
        public ImmutableArray3<K> set(int index, K obj) {
            if (level() < MAX_LEVEL) {
                return new Set<>(this, index, obj);
            }
            return Plain.set(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> insert(int index, K obj) {
            if (index == this.index) {
                return base.set(index, obj);
            } else if (level() < MAX_LEVEL) {
                return new Insert<>(this, index, obj);
            }
            return Plain.insert(this, index, obj);
        }

        @Override
        public ImmutableArray3<K> remove(int index) {
            if (level() < MAX_LEVEL) {
                return new Remove<>(this, index);
            }
            return Plain.remove(this, index);
        }

        @Override
        public int length() {
            return base.length() - 1;
        }

        @Override
        public K get(int index) {
            if (index < this.index) {
                return base.get(index);
            }
            return base.get(index + 1);
        }

        @Override
        int level() {
            return base.level() + 1;
        }

    }

}
