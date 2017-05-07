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

package org.apache.ignite.internal.binary.builder;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryMetadata;
import org.apache.ignite.internal.binary.BinaryObjectExImpl;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;

/**
 *
 */
class BinaryBuilderSerializer {
    /** */
    private final Map<BinaryObjectBuilderImpl, Integer> objToPos = new IdentityHashMap<>();

    /** */
    private Map<BinaryObject, BinaryObjectBuilderImpl> binaryObjToWrapper;

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
     * @return Field offset.
     */
    public int writeValue(BinaryWriterExImpl writer, Object val) {
        return writeValue(writer, val, false, false);
    }

    /**
     * @param writer Writer.
     * @param val Value.
     * @param forceCol Whether to force collection type.
     * @param forceMap Whether to force map type.
     * @return Value offset.
     */
    public int writeValue(BinaryWriterExImpl writer, Object val, boolean forceCol, boolean forceMap) {
        assert !(forceCol && forceMap);

        if (val == null)
            return BinaryUtils.NULL_4;

        int off = writer.currentOffset();

        if (val instanceof BinaryBuilderSerializationAware)
            return ((BinaryBuilderSerializationAware)val).writeTo(writer, this);

        if (val instanceof BinaryObjectExImpl) {
            if (binaryObjToWrapper == null)
                binaryObjToWrapper = new IdentityHashMap<>();

            BinaryObjectBuilderImpl wrapper = binaryObjToWrapper.get(val);

            if (wrapper == null) {
                wrapper = BinaryObjectBuilderImpl.wrap((BinaryObject)val);

                binaryObjToWrapper.put((BinaryObject)val, wrapper);
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

                writer.writeByte(GridBinaryMarshaller.HANDLE);
                writer.writeInt(handle);
            }

            return off;
        }

        if (val.getClass().isEnum()) {
            String clsName = val.getClass().getName();

            int typeId = writer.context().typeId(clsName);
            String typeName = writer.context().userTypeName(clsName);

            BinaryMetadata meta = new BinaryMetadata(typeId, typeName, null, null, null, true);

            writer.context().updateMetadata(typeId, meta);

            // Need register class for marshaller to be able to deserialize enum value.
            writer.context().descriptorForClass(val.getClass(), false);

            writer.writeByte(GridBinaryMarshaller.ENUM);
            writer.writeInt(typeId);
            writer.writeInt(((Enum)val).ordinal());

            return off;
        }

        if (forceCol || BinaryUtils.isSpecialCollection(val.getClass())) {
            Collection<?> c = (Collection<?>)val;

            writer.writeByte(GridBinaryMarshaller.COL);
            writer.writeInt(c.size());

            byte colType = writer.context().collectionType(c.getClass());

            writer.writeByte(colType);

            for (Object obj : c)
                writeValue(writer, obj);

            return off;
        }

        if (forceMap || BinaryUtils.isSpecialMap(val.getClass())) {
            Map<?, ?> map = (Map<?, ?>)val;

            writer.writeByte(GridBinaryMarshaller.MAP);
            writer.writeInt(map.size());

            writer.writeByte(writer.context().mapType(map.getClass()));

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                writeValue(writer, entry.getKey());
                writeValue(writer, entry.getValue());
            }

            return off;
        }

        if (BinaryUtils.PLAIN_CLASS_TO_FLAG.get(val.getClass()) != null)
            return BinaryUtils.writePlainObject(writer, val);

        if (val instanceof Object[]) {
            int compTypeId = writer.context().typeId(((Object[])val).getClass().getComponentType().getName());

            if (val instanceof BinaryBuilderEnum[]) {
                writeArray(writer, GridBinaryMarshaller.ENUM_ARR, (Object[])val, compTypeId);

                return off;
            }

            if (((Object[])val).getClass().getComponentType().isEnum()) {
                Enum[] enumArr = (Enum[])val;

                writer.writeByte(GridBinaryMarshaller.ENUM_ARR);
                writer.writeInt(compTypeId);
                writer.writeInt(enumArr.length);

                for (Enum anEnum : enumArr)
                    writeValue(writer, anEnum);

                return off;
            }

            writeArray(writer, GridBinaryMarshaller.OBJ_ARR, (Object[])val, compTypeId);

            return off;
        }

        writer.doWriteObject(val);

        return off;
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
        writer.writeInt(GridBinaryMarshaller.UNREGISTERED_TYPE_ID);
        writer.writeString(clsName);
        writer.writeInt(arr.length);

        for (Object obj : arr)
            writeValue(writer, obj);
    }

}
