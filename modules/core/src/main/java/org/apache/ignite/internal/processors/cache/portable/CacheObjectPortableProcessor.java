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

package org.apache.ignite.internal.processors.cache.portable;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.portable.api.IgnitePortables;
import org.apache.ignite.internal.processors.cacheobject.IgniteCacheObjectProcessor;
import org.apache.ignite.internal.portable.api.PortableBuilder;
import org.apache.ignite.internal.portable.api.PortableMetadata;
import org.apache.ignite.internal.portable.api.PortableObject;
import org.jetbrains.annotations.Nullable;

/**
 * Extended cache object processor interface with additional methods for portables.
 */
public interface CacheObjectPortableProcessor extends IgniteCacheObjectProcessor {
    /**
     * @param typeId Type ID.
     * @return Builder.
     */
    public PortableBuilder builder(int typeId);

    /**
     * @param clsName Class name.
     * @return Builder.
     */
    public PortableBuilder builder(String clsName);

    /**
     * Creates builder initialized by existing portable object.
     *
     * @param portableObj Portable object to edit.
     * @return Portable builder.
     */
    public PortableBuilder builder(PortableObject portableObj);

    /**
     * @param typeId Type ID.
     * @param newMeta New meta data.
     * @throws IgniteException In case of error.
     */
    public void addMeta(int typeId, final PortableMetadata newMeta) throws IgniteException;

    /**
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param affKeyFieldName Affinity key field name.
     * @param fieldTypeIds Fields map.
     * @throws IgniteException In case of error.
     */
    public void updateMetaData(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, Integer> fieldTypeIds) throws IgniteException;

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws IgniteException In case of error.
     */
    @Nullable public PortableMetadata metadata(int typeId) throws IgniteException;

    /**
     * @param typeIds Type ID.
     * @return Meta data.
     * @throws IgniteException In case of error.
     */
    public Map<Integer, PortableMetadata> metadata(Collection<Integer> typeIds) throws IgniteException;

    /**
     * @return Metadata for all types.
     * @throws IgniteException In case of error.
     */
    public Collection<PortableMetadata> metadata() throws IgniteException;

    /**
     * @return Portables interface.
     * @throws IgniteException If failed.
     */
    public IgnitePortables portables() throws IgniteException;

    /**
     * @param obj Original object.
     * @return Portable object (in case portable marshaller is used).
     * @throws IgniteException If failed.
     */
    public Object marshalToPortable(Object obj) throws IgniteException;
}