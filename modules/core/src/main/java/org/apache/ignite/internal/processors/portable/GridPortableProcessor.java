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

import org.apache.ignite.internal.processors.*;
import org.apache.ignite.portables.*;
import org.apache.ignite.client.marshaller.*;
import org.jetbrains.annotations.*;

import java.nio.*;
import java.util.*;

/**
 * Portable processor.
 */
public interface GridPortableProcessor extends GridProcessor {
    /**
     * @param typeName Type name.
     * @return Type ID.
     */
    public int typeId(String typeName);

    /**
     * @param obj Object to marshal.
     * @param trim If {@code true} trims result byte buffer.
     * @return Object bytes.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public ByteBuffer marshal(@Nullable Object obj, boolean trim) throws PortableException;

    /**
     * @param arr Byte array.
     * @param off Offset.
     * @return Unmarshalled object.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public Object unmarshal(byte[] arr, int off) throws PortableException;

    /**
     * @param ptr Offheap pointer.
     * @param forceHeap If {@code true} creates heap-based object.
     * @return Unmarshalled object.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public Object unmarshal(long ptr, boolean forceHeap) throws PortableException;

    /**
     * Converts temporary offheap object to heap-based.
     *
     * @param obj Object.
     * @return Heap-based object.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    @Nullable public Object unwrapTemporary(@Nullable Object obj) throws PortableException;

    /**
     * @param obj Object to marshal.
     * @return Portable object.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public Object marshalToPortable(@Nullable Object obj) throws PortableException;

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
     * @return Builder.
     */
    public PortableBuilder builder(int typeId);

    /**
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
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public void addMeta(int typeId, final PortableMetadata newMeta) throws PortableException;

    /**
     * @param typeId Type ID.
     * @param typeName Type name.
     * @param affKeyFieldName Affinity key field name.
     * @param fieldTypeIds Fields map.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public void updateMetaData(int typeId, String typeName, @Nullable String affKeyFieldName,
        Map<String, Integer> fieldTypeIds) throws PortableException;

    /**
     * @param typeId Type ID.
     * @return Meta data.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    @Nullable public PortableMetadata metadata(int typeId) throws PortableException;

    /**
     * @param typeIds Type ID.
     * @return Meta data.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public Map<Integer, PortableMetadata> metadata(Collection<Integer> typeIds) throws PortableException;

    /**
     * @return Metadata for all types.
     * @throws org.apache.ignite.portables.PortableException In case of error.
     */
    public Collection<PortableMetadata> metadata() throws PortableException;
}
