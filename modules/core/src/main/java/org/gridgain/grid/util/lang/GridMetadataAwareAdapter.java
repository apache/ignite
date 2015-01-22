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

package org.gridgain.grid.util.lang;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.tostring.*;
import org.jetbrains.annotations.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Convenient adapter for {@link IgniteMetadataAware}.
 * <h2 class="header">Thread Safety</h2>
 * This class provides necessary synchronization for thread-safe access.
 */
@SuppressWarnings( {"SynchronizeOnNonFinalField"})
public class GridMetadataAwareAdapter implements IgniteMetadataAware {
    /** */
    private static final long serialVersionUID = 0L;

    /** Attributes. */
    @GridToStringInclude
    private GridLeanMap<String, Object> data;

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
    public GridMetadataAwareAdapter(Map<String, Object> data) {
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

    /** {@inheritDoc} */
    @Override public void copyMeta(IgniteMetadataAware from) {
        A.notNull(from, "from");

        synchronized (mux) {
            Map m = from.allMeta();

            ensureData(m.size());

            data.putAll(from.allMeta());
        }
    }

    /** {@inheritDoc} */
    @Override public void copyMeta(Map<String, ?> data) {
        A.notNull(data, "data");

        synchronized (mux) {
            ensureData(data.size());

            this.data.putAll(data);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V> V addMeta(String name, V val) {
        A.notNull(name, "name", val, "val");

        synchronized (mux) {
            ensureData(1);

            return (V)data.put(name, val);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V> V meta(String name) {
        A.notNull(name, "name");

        synchronized (mux) {
            return data == null ? null : (V)data.get(name);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable
    @Override public <V> V removeMeta(String name) {
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

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V> boolean removeMeta(String name, V val) {
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

    /** {@inheritDoc} */
    @SuppressWarnings( {"unchecked", "RedundantCast"})
    @Override public <V> Map<String, V> allMeta() {
        synchronized (mux) {
            if (data == null)
                return Collections.emptyMap();

            if (data.size() <= 5)
                // This is a singleton unmodifiable map.
                return (Map<String, V>)data;

            // Return a copy.
            return new HashMap<>((Map<String, V>) data);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean hasMeta(String name) {
        return meta(name) != null;
    }

    /** {@inheritDoc} */
    @Override public <V> boolean hasMeta(String name, V val) {
        A.notNull(name, "name");

        Object v = meta(name);

        return v != null && v.equals(val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V> V putMetaIfAbsent(String name, V val) {
        A.notNull(name, "name", val, "val");

        synchronized (mux) {
            V v = (V) meta(name);

            if (v == null)
                return addMeta(name, val);

            return v;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked", "ClassReferencesSubclass"})
    @Nullable @Override public <V> V putMetaIfAbsent(String name, Callable<V> c) {
        A.notNull(name, "name", c, "c");

        synchronized (mux) {
            V v = (V) meta(name);

            if (v == null)
                try {
                    return addMeta(name, c.call());
                }
                catch (Exception e) {
                    throw F.wrap(e);
                }

            return v;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <V> V addMetaIfAbsent(String name, V val) {
        A.notNull(name, "name", val, "val");

        synchronized (mux) {
            V v = (V) meta(name);

            if (v == null)
                addMeta(name, v = val);

            return v;
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Nullable @Override public <V> V addMetaIfAbsent(String name, @Nullable Callable<V> c) {
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

    /** {@inheritDoc} */
    @SuppressWarnings({"RedundantTypeArguments"})
    @Override public <V> boolean replaceMeta(String name, V curVal, V newVal) {
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
        Map<String, Object> cp;

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
        GridLeanMap<String, Object> cp = (GridLeanMap<String, Object>)in.readObject();

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
