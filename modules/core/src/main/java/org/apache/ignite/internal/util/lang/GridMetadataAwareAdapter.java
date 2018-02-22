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

package org.apache.ignite.internal.util.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Convenient adapter for working with metadata. <h2 class="header">Thread Safety</h2> This class provides necessary
 * synchronization for thread-safe access.
 */
@SuppressWarnings({"SynchronizeOnNonFinalField"})
public class GridMetadataAwareAdapter {
    /**
     * Enum stored predefined keys.
     */
    public enum EntryKey {//keys sorted by usage rate, descending.
        /** Predefined key. */
        CACHE_STORE_MANAGER_KEY(0),

        /** Predefined key. */
        CACHE_EVICTABLE_ENTRY_KEY(1),

        /** Predefined key. */
        CACHE_EVICTION_MANAGER_KEY(2);

        /** key. */
        private int key;

        /**
         * @param key key
         */
        EntryKey(int key) {
            this.key = key;
        }

        /**
         * Returns key.
         *
         * @return key.
         */
        public int key() {
            return key;
        }
    }

    /** Attributes. */
    @GridToStringInclude(sensitive = true)
    private Object[] data = null;

    /**
     * Copies all metadata from another instance.
     *
     * @param from Metadata aware instance to copy metadata from.
     */
    public void copyMeta(GridMetadataAwareAdapter from) {
        assert from != null;

        copyMeta(from.allMeta());
    }

    /**
     * Copies all metadata from given map.
     *
     * @param data Map to copy metadata from.
     */
    public void copyMeta(Object[] data) {
        assert data != null;

        synchronized (this) {
            if (this.data.length < data.length)
                this.data = Arrays.copyOf(this.data, data.length);

            for (int k = 0; k < data.length; k++) {
                if (data[k] != null)
                    this.data[k] = data[k];
            }
        }
    }

    /**
     * Adds a new metadata.
     *
     * @param key Metadata key.
     * @param val Metadata value.
     * @param <V> Type of the value.
     * @return Metadata previously associated with given name, or {@code null} if there was none.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V addMeta(int key, V val) {
        assert val != null;

        synchronized (this) {
            if (this.data == null)
                this.data = new Object[key + 1];
            else if (this.data.length <= key)
                this.data = Arrays.copyOf(this.data, key + 1);

            V old = (V)data[key];

            data[key] = val;

            return old;
        }
    }

    /**
     * Gets metadata by name.
     *
     * @param key Metadata key.
     * @param <V> Type of the value.
     * @return Metadata value or {@code null}.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V meta(int key) {
        synchronized (this) {
            return data != null && data.length > key ? (V)data[key] : null;
        }
    }

    /**
     * Removes metadata by key.
     *
     * @param key Name of the metadata to remove.
     * @param <V> Type of the value.
     * @return Value of removed metadata or {@code null}.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V removeMeta(int key) {
        synchronized (this) {
            if (data == null || data.length <= key)
                return null;

            V old = (V)data[key];

            data[key] = null;

            return old;
        }
    }

    /**
     * Removes metadata only if its current value is equal to {@code val} passed in.
     *
     * @param key Name of metadata attribute.
     * @param val Value to compare.
     * @param <V> Value type.
     * @return {@code True} if value was removed, {@code false} otherwise.
     */
    @SuppressWarnings({"unchecked"})
    public <V> boolean removeMeta(int key, V val) {
        assert val != null;

        synchronized (this) {
            if (data == null || data.length <= key)
                return false;

            V old = (V)data[key];

            if (old != null && old.equals(val)) {
                data[key] = null;

                return true;
            }

            return false;
        }
    }

    /**
     * Gets all metadata in this entry.
     *
     * @param <V> Type of the value.
     * @return All metadata in this entry.
     */
    public <V> Object[] allMeta() {
        Object[] cp;

        synchronized (this) {
            cp = Arrays.copyOf(data, data.length);
        }

        return cp;
    }

    /**
     * Removes all meta.
     */
    public void removeAllMeta() {
        synchronized (this) {
            data = null;
        }
    }

    /**
     * Tests whether or not given metadata is set.
     *
     * @param key key of the metadata to test.
     * @return Whether or not given metadata is set.
     */
    public boolean hasMeta(int key) {
        return meta(key) != null;
    }

    /**
     * Tests whether or not given metadata is set.
     *
     * @param key Key of the metadata to test.
     * @return Whether or not given metadata is set.
     */
    public <V> boolean hasMeta(int key, V val) {
        Object v = meta(key);

        return v != null && v.equals(val);
    }

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param key Metadata key.
     * @param val Value to add if it's not attached already.
     * @param <V> Type of the value.
     * @return {@code null} if new value was put, or current value if put didn't happen.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V putMetaIfAbsent(int key, V val) {
        assert val != null;

        synchronized (this) {
            V v = (V)meta(key);

            if (v == null)
                return addMeta(key, val);

            return v;
        }
    }

    /**
     * Adds given metadata value only if it was absent. This method always returns the latest value and never previous
     * one.
     *
     * @param key Metadata key.
     * @param val Value to add if it's not attached already.
     * @param <V> Type of the value.
     * @return The value of the metadata after execution of this method.
     */
    @SuppressWarnings({"unchecked"})
    public <V> V addMetaIfAbsent(int key, V val) {
        assert val != null;

        synchronized (this) {
            V v = (V)meta(key);

            if (v == null)
                addMeta(key, v = val);

            return v;
        }
    }

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param key Metadata key.
     * @param c Factory closure to produce value to add if it's not attached already. Not that unlike {@link
     * #addMeta(int, Object)} method the factory closure will not be called unless the value is required and therefore
     * value will only be created when it is actually needed. If {@code null} and metadata value is missing - {@code
     * null} will be returned from this method.
     * @param <V> Type of the value.
     * @return The value of the metadata after execution of this method.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V addMetaIfAbsent(int key, @Nullable Callable<V> c) {
        assert c != null;

        synchronized (this) {
            V v = (V)meta(key);

            if (v == null && c != null)
                try {
                    addMeta(key, v = c.call());
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }

            return v;
        }
    }

    /**
     * Replaces given metadata with new {@code newVal} value only if its current value is equal to {@code curVal}.
     * Otherwise, it is no-op.
     *
     * @param key Key of the metadata.
     * @param curVal Current value to check.
     * @param newVal New value.
     * @return {@code true} if replacement occurred, {@code false} otherwise.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public <V> boolean replaceMeta(int key, V curVal, V newVal) {
        assert newVal != null;
        assert curVal != null;

        synchronized (this) {
            if (hasMeta(key)) {
                V val = this.<V>meta(key);

                if (val != null && val.equals(curVal)) {
                    addMeta(key, newVal);

                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Convenience way for super-classes which implement {@link Externalizable} to serialize metadata. Super-classes
     * must call this method explicitly from within {@link Externalizable#writeExternal(ObjectOutput)} methods
     * implementation.
     *
     * @param out Output to write to.
     * @throws IOException If I/O error occurred.
     */
    protected void writeExternalMeta(ObjectOutput out) throws IOException {
        Object[] cp;

        // Avoid code warning (suppressing is bad here, because we need this warning for other places).
        synchronized (this) {
            cp = Arrays.copyOf(this.data, this.data.length);
        }

        out.writeObject(cp);
    }

    /**
     * Convenience way for super-classes which implement {@link Externalizable} to serialize metadata. Super-classes
     * must call this method explicitly from within {@link Externalizable#readExternal(ObjectInput)} methods
     * implementation.
     *
     * @param in Input to read from.
     * @throws IOException If I/O error occurred.
     * @throws ClassNotFoundException If some class could not be found.
     */
    @SuppressWarnings({"unchecked"})
    protected void readExternalMeta(ObjectInput in) throws IOException, ClassNotFoundException {
        Object[] cp = (Object[])in.readObject();

        synchronized (this) {
            this.data = cp;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException", "OverriddenMethodCallDuringObjectConstruction"})
    @Override public Object clone() {
        try {
            GridMetadataAwareAdapter clone = (GridMetadataAwareAdapter)super.clone();

            clone.copyMeta(this);

            return clone;
        }
        catch (CloneNotSupportedException ignore) {
            throw new InternalError();
        }
    }
}