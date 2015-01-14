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

package org.gridgain.grid;

import org.jetbrains.annotations.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Allows to attach metadata attributes to any entity that implements
 * this interface. This promotes <i>meta-programming technique</i> throughout the
 * GridGain APIs by allowing to attach and query the <b>metadata information</b> to top-level
 * entities in GridGain APIs.
 * <p>
 * Note that unlike other types of attributes the metadata is explicitly
 * local to VM. Local metadata does not have any distribution semantic and is not
 * specifically distributed in any way. Note, however, that if object that implements this
 * interface gets serialized and deserialized on the remote node the attachments may or may not
 * be carried over (depending on the implementation). All classes that come with GridGain
 * and implement this interface support proper serialization and deserialization.
 * <p>
 * For example, this may become useful for cache entries or
 * cache transactions. Cache entry attachment can be used whenever entry
 * needs to carry additional context and it is too expensive to keep
 * looking that context up from a separate map by a key. For example,
 * an expiration policy used by caches may add some expiration
 * context to cache entries to properly expire them.
 */
public interface GridMetadataAware extends Serializable {
    /**
     * Copies all metadata from another instance.
     *
     * @param from Metadata aware instance to copy metadata from.
     */
    public void copyMeta(GridMetadataAware from);

    /**
     * Copies all metadata from given map.
     *
     * @param data Map to copy metadata from.
     */
    public void copyMeta(Map<String, ?> data);

    /**
     * Adds a new metadata.
     *
     * @param name Metadata name.
     * @param val Metadata value.
     * @param <V> Type of the value.
     * @return Metadata previously associated with given name, or
     *      {@code null} if there was none.
     */
    @Nullable public <V> V addMeta(String name, V val);

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param name Metadata name.
     * @param val Value to add if it's not attached already.
     * @param <V> Type of the value.
     * @return {@code null} if new value was put, or current value if put didn't happen.
     */
    @Nullable public <V> V putMetaIfAbsent(String name, V val);

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param name Metadata name.
     * @param c Factory closure to produce value to add if it's not attached already.
     *      Not that unlike {@link #addMeta(String, Object)} method the factory closure will
     *      not be called unless the value is required and therefore value will only be created
     *      when it is actually needed.
     * @param <V> Type of the value.
     * @return {@code null} if new value was put, or current value if put didn't happen.
     */
    @Nullable public <V> V putMetaIfAbsent(String name, Callable<V> c);

    /**
     * Adds given metadata value only if it was absent. Unlike
     * {@link #putMetaIfAbsent(String, Callable)}, this method always returns
     * the latest value and never previous one. 
     *
     * @param name Metadata name.
     * @param val Value to add if it's not attached already.
     * @param <V> Type of the value.
     * @return The value of the metadata after execution of this method.
     */
    public <V> V addMetaIfAbsent(String name, V val);

    /**
     * Adds given metadata value only if it was absent.
     *
     * @param name Metadata name.
     * @param c Factory closure to produce value to add if it's not attached already.
     *      Not that unlike {@link #addMeta(String, Object)} method the factory closure will
     *      not be called unless the value is required and therefore value will only be created
     *      when it is actually needed. If {@code null} and metadata value is missing - {@code null}
     *      will be returned from this method.
     * @param <V> Type of the value.
     * @return The value of the metadata after execution of this method.
     */
    @Nullable public <V> V addMetaIfAbsent(String name, @Nullable Callable<V> c);

    /**
     * Gets metadata by name.
     *
     * @param name Metadata name.
     * @param <V> Type of the value.
     * @return Metadata value or {@code null}.
     */
    public <V> V meta(String name);

    /**
     * Removes metadata by name.
     *
     * @param name Name of the metadata to remove.
     * @param <V> Type of the value.
     * @return Value of removed metadata or {@code null}.
     */
    public <V> V removeMeta(String name);

    /**
     * Removes metadata only if its current value is equal to {@code val} passed in.
     *
     * @param name Name of metadata attribute.
     * @param val Value to compare.
     * @param <V> Value type.
     * @return {@code True} if value was removed, {@code false} otherwise.
     */
    public <V> boolean removeMeta(String name, V val);

    /**
     * Gets all metadata in this entry.
     *
     * @param <V> Type of the value.
     * @return All metadata in this entry.
     */
    public <V> Map<String, V> allMeta();

    /**
     * Tests whether or not given metadata is set.
     *
     * @param name Name of the metadata to test.
     * @return Whether or not given metadata is set.
     */
    public boolean hasMeta(String name);

    /**
     * Tests whether or not metadata with given name and value is set. Returns {@code true} if
     * attachment with given name it set and its value is equals to provided value. Otherwise returns
     * {@code false}.
     *
     * @param name Name of the metadata.
     * @param val Value to compare. Cannot be {@code null}.
     * @param <V> Type of the value.
     * @return {@code true} if metadata with given name is set and its value is equals to provided value -
     *      otherwise returns {@code false}.
     */
    public <V> boolean hasMeta(String name, V val);

    /**
     * Replaces given metadata with new {@code newVal} value only if its current value
     * is equal to {@code curVal}. Otherwise, it is no-op.
     *
     * @param name Name of the metadata.
     * @param curVal Current value to check.
     * @param newVal New value.
     * @return {@code true} if replacement occurred, {@code false} otherwise.
     */
    public <V> boolean replaceMeta(String name, V curVal, V newVal);
}
