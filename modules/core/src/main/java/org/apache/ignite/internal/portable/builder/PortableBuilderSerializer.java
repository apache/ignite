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

package org.apache.ignite.internal.portable.builder;

import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.portable.BinaryMetadata;
import org.apache.ignite.internal.portable.BinaryObjectExImpl;
import org.apache.ignite.internal.portable.BinaryWriterExImpl;
import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableContext;
import org.apache.ignite.internal.portable.PortableUtils;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;

/**
 *
 */
class PortableBuilderSerializer {
    /** */
    private final Map<BinaryObjectBuilderImpl, Integer> objToPos = new IdentityHashMap<>();

    /** */
    private Map<BinaryObject, BinaryObjectBuilderImpl> portableObjToWrapper;

    /**
     * @param obj Mutable object.
     * @param posInResArr Object position in the array.
     */
    public void registerObjectWriting(BinaryObjectBuilderImpl obj, int posInResArr) {
        objToPos.put(obj, posInResArr);
    }

    /**
     * @param writer Writer.
     * @param val Value.
     */
    public void writeValue(BinaryWriterExImpl writer, Object val) {
        if (val == null) {
            writer.writeByte(GridPortableMarshaller.NULL);

            return;
        }

        if (val instanceof PortableBuilderSerializationAware) {
            ((PortableBuilderSerializationAware)val).writeTo(writer, this);

            return;
        }

        if (val instanceof BinaryObjectExImpl) {
            if (portableObjToWrapper == null)
                portableObjToWrapper = new IdentityHashMap<>();

            BinaryObjectBuilderImpl wrapper = portableObjToWrapper.get(val);

            if (wrapper == null) {
                wrapper = BinaryObjectBuilderImpl.wrap((BinaryObject)val);

                portableObjToWrapper.put((BinaryObject)val, wrapper);
            }

            val = wrapper;
        }

        if (val instanceof BinaryObjectBuilderImpl) {
            BinaryObjectBuilderImpl obj = (BinaryObjectBuilderImpl)val;

            Integer posInResArr = objToPos.get(obj);

            if (posInResArr == null) {
                objToPos.put(obj, writer.out().position());

                obj.serializeTo(writer.newWriter(obj.typeId()), this);
            }
            else {
                int handle = writer.out().position() - posInResArr;

                writer.writeByte(GridPortableMarshaller.HANDLE);
                writer.writeInt(handle);
            }

            return;
        }

        if (val.getClass().isEnum()) {
            String typeName = PortableContext.typeName(val.getClass().getName());
            int typeId = writer.context().typeId(typeName);

            BinaryMetadata meta = new BinaryMetadata(typeId, typeName, null, null, null, true);
            writer.context().updateMetadata(typeId, meta);

            writer.writeByte(GridPortableMarshaller.ENUM);
            writer.writeInt(typeId);
            writer.writeInt(((Enum)val).ordinal());

            return;
        }

        if (val instanceof Collection) {
            Collection<?> c = (Collection<?>)val;

            writer.writeByte(GridPortableMarshaller.COL);
            writer.writeInt(c.size());

            byte colType = writer.context().collectionType(c.getClass());

            writer.writeByte(colType);

            for (Object obj : c)
                writeValue(writer, obj);

            return;
        }

        if (val instanceof Map) {
            Map<?, ?> map = (Map<?, ?>)val;

            writer.writeByte(GridPortableMarshaller.MAP);
            writer.writeInt(map.size());

            writer.writeByte(writer.context().mapType(map.getClass()));

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                writeValue(writer, entry.getKey());
                writeValue(writer, entry.getValue());
            }

            return;
        }

        Byte flag = PortableUtils.PLAIN_CLASS_TO_FLAG.get(val.getClass());

        if (flag != null) {
            PortableUtils.writePlainObject(writer, val);

            return;
        }

        if (val instanceof Object[]) {
            int compTypeId = writer.context().typeId(((Object[])val).getClass().getComponentType().getName());

            if (val instanceof PortableBuilderEnum[]) {
                writeArray(writer, GridPortableMarshaller.ENUM_ARR, (Object[])val, compTypeId);

                return;
            }

            if (((Object[])val).getClass().getComponentType().isEnum()) {
                Enum[] enumArr = (Enum[])val;

                writer.writeByte(GridPortableMarshaller.ENUM_ARR);
                writer.writeInt(compTypeId);
                writer.writeInt(enumArr.length);

                for (Enum anEnum : enumArr)
                    writeValue(writer, anEnum);

                return;
            }

            writeArray(writer, GridPortableMarshaller.OBJ_ARR, (Object[])val, compTypeId);

            return;
        }

        writer.doWriteObject(val);
    }

    /**
     * @param writer Writer.
     * @param elementType Element type.
     * @param arr The array.
     * @param compTypeId Component type ID.
     */
    public void writeArray(BinaryWriterExImpl writer, byte elementType, Object[] arr, int compTypeId) {
        writer.writeByte(elementType);
        writer.writeInt(compTypeId);
        writer.writeInt(arr.length);

        for (Object obj : arr)
            writeValue(writer, obj);
    }

    /**
     * @param writer Writer.
     * @param elementType Element type.
     * @param arr The array.
     * @param clsName Component class name.
     */
    public void writeArray(BinaryWriterExImpl writer, byte elementType, Object[] arr, String clsName) {
        writer.writeByte(elementType);
        writer.writeInt(GridPortableMarshaller.UNREGISTERED_TYPE_ID);
        writer.writeString(clsName);
        writer.writeInt(arr.length);

        for (Object obj : arr)
            writeValue(writer, obj);
    }

}