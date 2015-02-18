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

package org.apache.ignite.internal.processors.portable;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.client.marshaller.*;
import org.apache.ignite.internal.processors.*;
import org.jetbrains.annotations.*;

import java.nio.*;

/**
 * Portable processor.
 */
public interface GridPortableProcessor extends GridProcessor {
    /** {@inheritDoc} */
    public void onCacheProcessorStarted();

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
     * @param obj Object to marshal.
     * @param trim If {@code true} trims result byte buffer.
     * @return Object bytes.
     * @throws IgniteException In case of error.
     */
    public ByteBuffer marshal(@Nullable Object obj, boolean trim) throws IgniteException;

    /**
     * @param arr Byte array.
     * @param off Offset.
     * @return Unmarshalled object.
     * @throws IgniteException In case of error.
     */
    public Object unmarshal(byte[] arr, int off) throws IgniteException;

    /**
     * @param ptr Offheap pointer.
     * @param forceHeap If {@code true} creates heap-based object.
     * @return Unmarshalled object.
     * @throws IgniteException In case of error.
     */
    public Object unmarshal(long ptr, boolean forceHeap) throws IgniteException;

    /**
     * Converts temporary offheap object to heap-based.
     *
     * @param obj Object.
     * @return Heap-based object.
     * @throws IgniteException In case of error.
     */
    @Nullable public Object unwrapTemporary(@Nullable Object obj) throws IgniteException;

    /**
     * @param obj Object to marshal.
     * @return Portable object.
     * @throws IgniteException In case of error.
     */
    public Object marshalToPortable(@Nullable Object obj) throws IgniteException;

    /**
     * @param obj Object (portable or not).
     * @return Detached portable object or original object.
     */
    public Object detachPortable(@Nullable Object obj);

    /**
     * @return Portable marshaller for client connectivity or {@code null} if it's not
     *      supported (in case of OS edition).
     */
    @Nullable public GridClientMarshaller portableMarshaller();

    /**
     * @param marsh Client marshaller.
     * @return Whether marshaller is portable.
     */
    public boolean isPortable(GridClientMarshaller marsh);

    /**
     * @param node Node to check.
     * @param cacheName Cache name to check.
     * @return {@code True} if portable enabled for the specified cache, {@code false} otherwise.
     */
    public boolean portableEnabled(ClusterNode node, String cacheName);

    /**
     * Checks whether object is portable object.
     *
     * @param obj Object to check.
     * @return {@code True} if object is already a portable object, {@code false} otherwise.
     */
    public boolean isPortableObject(Object obj);

    /**
     * Gets affinity key of portable object.
     *
     * @param obj Object to get affinity key for.
     * @return Affinity key.
     */
    public Object affinityKey(Object obj);

    /**
     * @param obj Portable object to get field from.
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
}
