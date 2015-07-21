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

package org.apache.ignite.internal.processors.cacheobject;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.marshaller.optimized.*;
import org.jetbrains.annotations.*;

import java.lang.reflect.*;

/**
 * Cache objects processor.
 */
public interface IgniteCacheObjectProcessor extends GridProcessor {
    /**
     * @see GridComponent#onKernalStart()
     * @throws IgniteCheckedException If failed.
     */
    public void onUtilityCacheStarted() throws IgniteCheckedException;

    /**
     * Callback invoked when cache is started.
     *
     * @param cctx Cache context.
     */
    public void onCacheStart(GridCacheContext cctx);

    public void onCacheStop(GridCacheContext cctx);

    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName);

    /**
     * @param obj Object to get type ID for.
     * @return Type ID.
     */
    public int typeId(Object obj);

    /**
     * Converts temporary off-heap object to heap-based.
     *
     * @param ctx Context.
     * @param obj Object.
     * @return Heap-based object.
     * @throws IgniteException In case of error.
     */
    @Nullable public Object unwrapTemporary(GridCacheContext ctx, @Nullable Object obj) throws IgniteException;

    /**
     * Unwraps indexed object into user object.
     *
     * @param obj Indexed object.
     * @return Unwrapped user object.
     * @throws IgniteException If unwrap failed.
     */
    @Nullable public Object unwrapIndexedObject(Object obj) throws IgniteException;

    /**
     * Prepares cache object for cache (e.g. copies user-provided object if needed).
     *
     * @param obj Cache object.
     * @param cctx Cache context.
     * @return Object to be store in cache.
     */
    @Nullable public CacheObject prepareForCache(@Nullable CacheObject obj, GridCacheContext cctx);

    /**
     * Checks whether objects supports index fields extraction.
     *
     * @param obj Object to check.
     * @return {@code True} if object is indexed object, {@code false} otherwise.
     */
    public boolean isIndexedObject(Object obj);

    /**
     * Gets affinity key for the given cache object.
     *
     * @param idxObj Indexed object to get affinity key for.
     * @return Affinity key for the given cache key object.
     */
    public Object affinityKey(CacheIndexedObject idxObj);

    /**
     * Checks whether this node has class representing the given type ID.
     *
     * @param typeId Type ID to check.
     * @return {@code True} if class for the given type ID is available.
     */
    public boolean hasClass(int typeId);

    /**
     * Checks whether the given class needs to be potentially unwrapped.
     *
     * @param cls Class to check.
     * @return {@code True} if needs to be unwrapped.
     */
    boolean isIndexedObjectOrCollectionType(Class<?> cls);

    /**
     * Checks whether this functionality is globally supported.
     *
     * @return {@code true} if enabled.
     */
    public boolean isFieldsIndexingEnabled();

    /**
     * Checks whether fields indexing is supported by footer injection into a serialized form of the object.
     * Footer contains information about fields location in the serialized form, thus enabling fast queries without
     * a need to deserialize the object.
     *
     * @param cls Class.
     * @return {@code true} if the footer is enabled.
     */
    public boolean isFieldsIndexingSupported(Class<?> cls);

    /**
     * @param ctx Cache object context.
     * @param val Value.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] marshal(CacheObjectContext ctx, Object val) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @param bytes Bytes.
     * @param clsLdr Class loader.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If failed.
     */
    public Object unmarshal(CacheObjectContext ctx, byte[] bytes, ClassLoader clsLdr) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @param bytes Bytes.
     * @param off Offset.
     * @param len Length.
     * @param clsLdr Class loader.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If failed.
     */
    public Object unmarshal(CacheObjectContext ctx, byte[] bytes, int off, int len, ClassLoader clsLdr)
        throws IgniteCheckedException;

    /**
     * @param ccfg Cache configuration.
     * @return Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    public CacheObjectContext contextForCache(CacheConfiguration ccfg) throws IgniteCheckedException;

    /**
     * @param obj Key value.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Cache key object.
     */
    public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx, Object obj, boolean userObj);

    /**
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Cache object.
     */
    @Nullable public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj, boolean userObj);

    /**
     * @param type Object type.
     * @param bytes Object bytes.
     * @return Cache object.
     */
    public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes);

    /**
     * @param ctx Context.
     * @param valPtr Value pointer.
     * @param tmp If {@code true} can return temporary instance which is valid while entry lock is held.
     * @return Cache object.
     * @throws IgniteCheckedException If failed.
     */
    public CacheObject toCacheObject(GridCacheContext ctx, long valPtr, boolean tmp) throws IgniteCheckedException;

    /**
     * @param obj Value.
     * @return {@code True} if object is of known immutable type.
     */
    public boolean immutable(Object obj);
}
