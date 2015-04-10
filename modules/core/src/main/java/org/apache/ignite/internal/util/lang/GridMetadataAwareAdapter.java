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

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Convenient adapter for working with metadata.
 * <h2 class="header">Thread Safety</h2>
 * This class provides necessary synchronization for thread-safe access.
 */
@SuppressWarnings( {"SynchronizeOnNonFinalField"})
public class GridMetadataAwareAdapter {
    /** Attributes. */
    @GridToStringInclude
    private GridLeanMap<UUID, Object> data;

    /** Serializable mutex. */
    @SuppressWarnings( {"FieldAccessedSynchronizedAndUnsynchronized"})
    private GridMutex mux;

    /**
     * Default constructor.
     */
    public GridMetadataAwareAdapter() {
        mux = new GridMutex();
    }

    /**
     * Creates adapter with predefined data.
     *
     * @param data Data to copy.
     */
    public GridMetadataAwareAdapter(Map<UUID, Object> data) {
        mux = new GridMutex();

        if (data != null && !data.isEmpty())
            this.data = new GridLeanMap<>(data);
    }

    /**
     * Ensures that internal data storage is created.
     *
     * @param size Amount of data to ensure.
     * @return {@code true} if data storage was created.
     */
    private boolean ensureData(int size) {
        if (data == null) {
            data = new GridLeanMap<>(size);

            return true;
        }
        else
            return false;
    }

    /**
     * Copies all metadata from another instance.
     *
     * @param from Metadata aware instance to copy metadata from.
     */
    public void copyMeta(GridMetadataAwareAdapter from) {
        A.notNull(from, "from");

        synchronized (mux) {
            Map m = from.allMeta();

            ensureData(m.size());

            data.putAll(from.allMeta());
        }
    }

    /**
     * Copies all metadata from given map.
     *
     * @param data Map to copy metadata from.
     */
    public void copyMeta(Map<UUID, ?> data) {
        A.notNull(data, "data");

        synchronized (mux) {
            ensureData(data.size());

            this.data.putAll(data);
        }
    }

    /**
     * Adds a new metadata.
     *
     * @param name Metadata name.
     * @param val Metadata value.
     * @param <V> Type of the value.
     * @return Metadata previously associated with given name, or
     *      {@code null} if there was none.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V addMeta(UUID name, V val) {
        A.notNull(name, "name", val, "val");

        synchronized (mux) {
            ensureData(1);

            return (V)data.put(name, val);
        }
    }

    /**
     * Gets metadata by name.
     *
     * @param name Metadata name.
     * @param <V> Type of the value.
     * @return Metadata value or {@code null}.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V meta(UUID name) {
        A.notNull(name, "name");

        synchronized (mux) {
            return data == null ? null : (V)data.get(name);
        }
    }

    /**
     * Removes metadata by name.
     *
     * @param name Name of the metadata to remove.
     * @param <V> Type of the value.
     * @return Value of removed metadata or {@code null}.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V removeMeta(UUID name) {
        A.notNull(name, "name");

        synchronized (mux) {
            if (data == null)
                return null;

            V old = (V)data.remove(name);

            if (data.isEmpty())
                data = null;

            return old;
        }
    }

    /**
     * Removes metadata only if its current value is equal to {@code val} passed in.
     *
     * @param name Name of metadata attribute.
     * @param val Value to compare.
     * @param <V> Value type.
     * @return {@code True} if value was removed, {@code false} otherwise.
     */
    @SuppressWarnings({"unchecked"})
    public <V> boolean removeMeta(UUID name, V val) {
        A.notNull(name, "name", val, "val");

        synchronized (mux) {
            if (data == null)
                return false;

            V old = (V)data.get(name);

            if (old != null && old.equals(val)) {
                data.remove(name);

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
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    public <V> Map<UUID, V> allMeta() {
        synchronized (mux) {
            if (data == null)
                return Collections.emptyMap();

            if (data.size() <= 5)
                // This is a singleton unmodifiable map.
                return (Map<UUID, V>)data;

            // Return a copy.
            return new HashMap<>((Map<UUID, V>) data);
        }
    }

    /**
     * Tests whether or not given metadata is set.
     *
     * @param name Name of the metadata to test.
     * @return Whether or not given metadata is set.
     */
    public boolean hasMeta(UUID name) {
        return meta(name) != null;
    }

    /**
     * Tests whether or not given metadata is set.
     *
     * @param name Name of the metadata to test.
     * @return Whether or not given metadata is set.
     */
    public <V> boolean hasMeta(UUID name, V val) {
        A.notNull(name, "name");

        Object v = meta(name);

        return v != null && v.equals(val);
    }

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param name Metadata name.
     * @param val Value to add if it's not attached already.
     * @param <V> Type of the value.
     * @return {@code null} if new value was put, or current value if put didn't happen.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V putMetaIfAbsent(UUID name, V val) {
        A.notNull(name, "name", val, "val");

        synchronized (mux) {
            V v = (V) meta(name);

            if (v == null)
                return addMeta(name, val);

            return v;
        }
    }

    /**
     * Adds given metadata value only if it was absent. This method always returns
     * the latest value and never previous one.
     *
     * @param name Metadata name.
     * @param val Value to add if it's not attached already.
     * @param <V> Type of the value.
     * @return The value of the metadata after execution of this method.
     */
    @SuppressWarnings({"unchecked"})
    public <V> V addMetaIfAbsent(UUID name, V val) {
        A.notNull(name, "name", val, "val");

        synchronized (mux) {
            V v = (V) meta(name);

            if (v == null)
                addMeta(name, v = val);

            return v;
        }
    }

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param name Metadata name.
     * @param c Factory closure to produce value to add if it's not attached already.
     *      Not that unlike {@link #addMeta(UUID, Object)} method the factory closure will
     *      not be called unless the value is required and therefore value will only be created
     *      when it is actually needed. If {@code null} and metadata value is missing - {@code null}
     *      will be returned from this method.
     * @param <V> Type of the value.
     * @return The value of the metadata after execution of this method.
     */
    @SuppressWarnings({"unchecked"})
    @Nullable public <V> V addMetaIfAbsent(UUID name, @Nullable Callable<V> c) {
        A.notNull(name, "name", c, "c");

        synchronized (mux) {
            V v = (V) meta(name);

            if (v == null && c != null)
                try {
                    addMeta(name, v = c.call());
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }

            return v;
        }
    }

    /**
     * Replaces given metadata with new {@code newVal} value only if its current value
     * is equal to {@code curVal}. Otherwise, it is no-op.
     *
     * @param name Name of the metadata.
     * @param curVal Current value to check.
     * @param newVal New value.
     * @return {@code true} if replacement occurred, {@code false} otherwise.
     */
    @SuppressWarnings({"RedundantTypeArguments"})
    public <V> boolean replaceMeta(UUID name, V curVal, V newVal) {
        A.notNull(name, "name", newVal, "newVal", curVal, "curVal");

        synchronized (mux) {
            if (hasMeta(name)) {
                V val = this.<V>meta(name);

                if (val != null && val.equals(curVal)) {
                    addMeta(name, newVal);

                    return true;
                }
            }

            return false;
        }
    }

    /**
     * Convenience way for super-classes which implement {@link Externalizable} to
     * serialize metadata. Super-classes must call this method explicitly from
     * within {@link Externalizable#writeExternal(ObjectOutput)} methods implementation.
     *
     * @param out Output to write to.
     * @throws IOException If I/O error occurred.
     */
    protected void writeExternalMeta(ObjectOutput out) throws IOException {
        Map<UUID, Object> cp;

        // Avoid code warning (suppressing is bad here, because we need this warning for other places).
        synchronized (mux) {
            cp = new GridLeanMap<>(data);
        }

        out.writeObject(cp);
    }

    /**
     * Convenience way for super-classes which implement {@link Externalizable} to
     * serialize metadata. Super-classes must call this method explicitly from
     * within {@link Externalizable#readExternal(ObjectInput)} methods implementation.
     *
     * @param in Input to read from.
     * @throws IOException If I/O error occurred.
     * @throws ClassNotFoundException If some class could not be found.
     */
    @SuppressWarnings({"unchecked"})
    protected void readExternalMeta(ObjectInput in) throws IOException, ClassNotFoundException {
        GridLeanMap<UUID, Object> cp = (GridLeanMap<UUID, Object>)in.readObject();

        synchronized (mux) {
            data = cp;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException", "OverriddenMethodCallDuringObjectConstruction"})
    @Override public Object clone() {
        try {
            GridMetadataAwareAdapter clone = (GridMetadataAwareAdapter)super.clone();

            clone.mux = (GridMutex)mux.clone();

            clone.data = null;

            clone.copyMeta(this);

            return clone;
        }
        catch (CloneNotSupportedException ignore) {
            throw new InternalError();
        }
    }
}
