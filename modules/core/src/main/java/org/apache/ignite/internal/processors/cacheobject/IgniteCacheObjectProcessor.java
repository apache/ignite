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

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.processors.GridProcessor;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectContext;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.IncompleteCacheObject;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.jetbrains.annotations.Nullable;

/**
 * Cache objects processor.
 */
public interface IgniteCacheObjectProcessor extends GridProcessor {
    /**
     * @param ctx Context.
     * @throws IgniteCheckedException If failed.
     */
    public void onContinuousProcessorStarted(GridKernalContext ctx) throws IgniteCheckedException;

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
     * Prepares cache object for cache (e.g. copies user-provided object if needed).
     *
     * @param obj Cache object.
     * @param cctx Cache context.
     * @return Object to be store in cache.
     */
    @Nullable public CacheObject prepareForCache(@Nullable CacheObject obj, GridCacheContext cctx);

    /**
     * Checks whether object is binary object.
     *
     * @param obj Object to check.
     * @return {@code True} if object is already a binary object, {@code false} otherwise.
     */
    public boolean isBinaryObject(Object obj);

    /**
     * Checks whether given class is binary.
     *
     * @return {@code true} If binary objects are enabled.
     */
    public boolean isBinaryEnabled(CacheConfiguration<?, ?> ccfg);

    /**
     * @param obj Binary object to get field from.
     * @param fieldName Field name.
     * @return Field value.
     */
    public Object field(Object obj, String fieldName);

    /**
     * Checks whether field is set in the object.
     *
     * @param obj Object.
     * @param fieldName Field name.
     * @return {@code true} if field is set.
     */
    public boolean hasField(Object obj, String fieldName);

    /**
     * @param ctx Cache object context.
     * @param val Value.
     * @return Value bytes.
     * @throws IgniteCheckedException If failed.
     */
    public byte[] marshal(CacheObjectValueContext ctx, Object val) throws IgniteCheckedException;

    /**
     * @param ctx Context.
     * @param bytes Bytes.
     * @param clsLdr Class loader.
     * @return Unmarshalled object.
     * @throws IgniteCheckedException If failed.
     */
    public Object unmarshal(CacheObjectValueContext ctx, byte[] bytes, ClassLoader clsLdr)
        throws IgniteCheckedException;

    /**
     * @param ccfg Cache configuration.
     * @return Cache object context.
     * @throws IgniteCheckedException If failed.
     */
    public CacheObjectContext contextForCache(CacheConfiguration ccfg) throws IgniteCheckedException;

    /**
     * @param ctx Cache objects context.
     * @param cctx Cache context if cache is available.
     * @param obj Key value.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Cache key object.
     */
    public KeyCacheObject toCacheKeyObject(CacheObjectContext ctx,
        @Nullable GridCacheContext cctx,
        Object obj,
        boolean userObj);

    /**
     * @param ctx Cache context.
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @return Cache object.
     */
    @Nullable public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj, boolean userObj);

    /**
     * @param ctx Cache context.
     * @param obj Object.
     * @param userObj If {@code true} then given object is object provided by user and should be copied
     *        before stored in cache.
     * @param failIfUnregistered Throw exception if class isn't registered.
     * @return Cache object.
     */
    @Nullable public CacheObject toCacheObject(CacheObjectContext ctx, @Nullable Object obj, boolean userObj,
        boolean failIfUnregistered);

    /**
     * @param ctx Cache context.
     * @param type Object type.
     * @param bytes Object bytes.
     * @return Cache object.
     */
    public CacheObject toCacheObject(CacheObjectContext ctx, byte type, byte[] bytes);

    /**
     * @param ctx Cache context.
     * @param type Object type.
     * @param bytes Object bytes.
     * @return Cache object.
     */
    public KeyCacheObject toKeyCacheObject(CacheObjectContext ctx, byte type, byte[] bytes) throws IgniteCheckedException;

    /**
     * @param ctx Cache context.
     * @param buf Buffer to read from.
     * @return Cache object.
     */
    public CacheObject toCacheObject(CacheObjectContext ctx, ByteBuffer buf);

    /**
     * @param ctx Cache object context.
     * @param buf Buffer.
     * @param incompleteObj Incomplete cache object or {@code null} if it's a first read.
     * @return Incomplete cache object.
     * @throws IgniteCheckedException If fail.
     */
    public IncompleteCacheObject toCacheObject(CacheObjectContext ctx, ByteBuffer buf,
        @Nullable IncompleteCacheObject incompleteObj) throws IgniteCheckedException;

    /**
     * @param ctx Cache object context.
     * @param buf Buffer.
     * @param incompleteObj Incomplete cache object or {@code null} if it's a first read.
     * @return Incomplete cache object.
     * @throws IgniteCheckedException If fail.
     */
    public IncompleteCacheObject toKeyCacheObject(CacheObjectContext ctx, ByteBuffer buf,
        @Nullable IncompleteCacheObject incompleteObj) throws IgniteCheckedException;

    /**
     * @param obj Value.
     * @return {@code True} if object is of known immutable type.
     */
    public boolean immutable(Object obj);

    /**
     * @return Ignite binary interface.
     * @throws IgniteException If failed.
     */
    public IgniteBinary binary() throws IgniteException;

    /**
     * @param clsName Class name.
     * @return Builder.
     */
    public BinaryObjectBuilder builder(String clsName);

    /**
     * Creates builder initialized by existing binary object.
     *
     * @param binaryObj Binary object to edit.
     * @return Binary builder.
     */
    public BinaryObjectBuilder builder(BinaryObject binaryObj);

    /**
     * @param typeId Type ID.
     * @param newMeta New metadata.
     * @param failIfUnregistered Fail if unregistered.
     * @throws IgniteException In case of error.
     */
    public void addMeta(int typeId, final BinaryType newMeta, boolean failIfUnregistered) throws IgniteException;

    /**
     * Adds metadata locally without triggering discovery exchange.
     *
     * Must be used only during startup and only if it is guaranteed that all nodes have the same copy
     * of BinaryType.
     *
     * @param typeId Type ID.
     * @param newMeta New metadata.
     * @throws IgniteException In case of error.
     */
    public void addMetaLocally(int typeId, final BinaryType newMeta) throws IgniteException;

    /**
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param affKeyFieldName Affinity key field name.
     * @param fieldTypeIds Fields map.
     * @param isEnum Enum flag.
     * @param enumMap Enum name to ordinal mapping.
     * @throws IgniteException In case of error.
     */
    public void updateMetadata(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, BinaryFieldMetadata> fieldTypeIds, boolean isEnum, @Nullable Map<String, Integer> enumMap)
        throws IgniteException;

    /**
     * @param typeId Type ID.
     * @return Metadata.
     * @throws IgniteException In case of error.
     */
    @Nullable public BinaryType metadata(int typeId) throws IgniteException;


    /**
     * @param typeId Type ID.
     * @param schemaId Schema ID.
     * @return Metadata.
     * @throws IgniteException In case of error.
     */
    @Nullable public BinaryType metadata(int typeId, int schemaId) throws IgniteException;

    /**
     * @param typeIds Type ID.
     * @return Metadata.
     * @throws IgniteException In case of error.
     */
    public Map<Integer, BinaryType> metadata(Collection<Integer> typeIds) throws IgniteException;

    /**
     * @return Metadata for all types.
     * @throws IgniteException In case of error.
     */
    public Collection<BinaryType> metadata() throws IgniteException;

    /**
     * @param types Collection of binary types to write to.
     * @param dir Destination directory.
     */
    public void saveMetadata(Collection<BinaryType> types, File dir);

    /**
     * @param typeName Type name.
     * @param ord ordinal.
     * @return Enum object.
     * @throws IgniteException If failed.
     */
    public BinaryObject buildEnum(String typeName, int ord) throws IgniteException;

    /**
     * @param typeName Type name.
     * @param name Name.
     * @return Enum object.
     * @throws IgniteException If failed.
     */
    public BinaryObject buildEnum(String typeName, String name) throws IgniteException;

    /**
     * Register enum type
     *
     * @param typeName Type name.
     * @param vals Mapping of enum constant names to ordinals.
     * @return Binary Type for registered enum.
     */
    public BinaryType registerEnum(String typeName, Map<String, Integer> vals) throws IgniteException;

    /**
     * @param obj Original object.
     * @param failIfUnregistered Throw exception if class isn't registered.
     * @return Binary object (in case binary marshaller is used).
     * @throws IgniteException If failed.
     */
    public Object marshalToBinary(Object obj, boolean failIfUnregistered) throws IgniteException;

    /**
     * Remove registered binary type from grid.
     *
     * Attention: this is not safe feature, the grid must not contain binary objects
     * with specified type, operations with specified type must not be processed on the cluster.
     *
     * @param typeId Type ID.
     */
    public void removeType(int typeId);
}
