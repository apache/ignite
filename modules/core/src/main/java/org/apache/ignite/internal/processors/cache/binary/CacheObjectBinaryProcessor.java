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

package org.apache.ignite.internal.processors.cache.binary;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.internal.binary.BinaryFieldMetadata;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * Extended cache object processor interface with additional methods for binary.
 */
public interface CacheObjectBinaryProcessor extends IgniteCacheObjectProcessor {
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
     * @param newMeta New meta data.
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
     * @param newMeta New meta data.
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
     * @return Meta data.
     * @throws IgniteException In case of error.
     */
    @Nullable public BinaryType metadata(int typeId) throws IgniteException;


    /**
     * @param typeId Type ID.
     * @param schemaId Schema ID.
     * @return Meta data.
     * @throws IgniteException In case of error.
     */
    @Nullable public BinaryType metadata(int typeId, int schemaId) throws IgniteException;

    /**
     * @param typeIds Type ID.
     * @return Meta data.
     * @throws IgniteException In case of error.
     */
    public Map<Integer, BinaryType> metadata(Collection<Integer> typeIds) throws IgniteException;

    /**
     * @return Metadata for all types.
     * @throws IgniteException In case of error.
     */
    public Collection<BinaryType> metadata() throws IgniteException;

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
     * @return Binaries interface.
     * @throws IgniteException If failed.
     */
    @Override public IgniteBinary binary() throws IgniteException;

    /**
     * @param obj Original object.
     * @param failIfUnregistered Throw exception if class isn't registered.
     * @return Binary object (in case binary marshaller is used).
     * @throws IgniteException If failed.
     */
    public Object marshalToBinary(Object obj, boolean failIfUnregistered) throws IgniteException;
}
