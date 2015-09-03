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

import org.apache.ignite.internal.portable.GridPortableMarshaller;
import org.apache.ignite.internal.portable.PortableObjectEx;
import org.apache.ignite.internal.portable.PortableUtils;
import org.apache.ignite.internal.portable.PortableWriterExImpl;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.portable.*;

import java.util.*;

/**
 *
 */
class PortableBuilderSerializer {
    /** */
    private final Map<PortableBuilderImpl, Integer> objToPos = new IdentityHashMap<>();

    /** */
    private Map<PortableObject, PortableBuilderImpl> portableObjToWrapper;

    /**
     * @param obj Mutable object.
     * @param posInResArr Object position in the array.
     */
    public void registerObjectWriting(PortableBuilderImpl obj, int posInResArr) {
        objToPos.put(obj, posInResArr);
    }

    /**
     * @param writer Writer.
     * @param val Value.
     */
    public void writeValue(PortableWriterExImpl writer, Object val) {
        if (val == null) {
            writer.writeByte(GridPortableMarshaller.NULL);

            return;
        }

        if (val instanceof PortableBuilderSerializationAware) {
            ((PortableBuilderSerializationAware)val).writeTo(writer, this);

            return;
        }

        if (val instanceof PortableObjectEx) {
            if (portableObjToWrapper == null)
                portableObjToWrapper = new IdentityHashMap<>();

            PortableBuilderImpl wrapper = portableObjToWrapper.get(val);

            if (wrapper == null) {
                wrapper = PortableBuilderImpl.wrap((PortableObject)val);

                portableObjToWrapper.put((PortableObject)val, wrapper);
            }

            val = wrapper;
        }

        if (val instanceof PortableBuilderImpl) {
            PortableBuilderImpl obj = (PortableBuilderImpl)val;

            Integer posInResArr = objToPos.get(obj);

            if (posInResArr == null) {
                objToPos.put(obj, writer.outputStream().position());

                obj.serializeTo(writer.newWriter(obj.typeId()), this);
            }
            else {
                int handle = writer.outputStream().position() - posInResArr;

                writer.writeByte(GridPortableMarshaller.HANDLE);
                writer.writeInt(handle);
            }

            return;
        }

        if (val.getClass().isEnum()) {
            writer.writeByte(GridPortableMarshaller.ENUM);
            writer.writeInt(writer.context().typeId(val.getClass().getName()));
            writer.writeInt(((Enum)val).ordinal());

            return;
        }

        if (val instanceof Collection) {
            Collection<?> c = (Collection<?>)val;

            writer.writeByte(GridPortableMarshaller.COL);
            writer.writeInt(c.size());

            byte colType;

            if (c instanceof GridConcurrentSkipListSet)
                colType = GridPortableMarshaller.CONC_SKIP_LIST_SET;
            else
                colType = writer.context().collectionType(c.getClass());


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

        writer.doWriteObject(val, false);
    }

    /**
     * @param writer Writer.
     * @param elementType Element type.
     * @param arr The array.
     * @param compTypeId Component type ID.
     */
    public void writeArray(PortableWriterExImpl writer, byte elementType, Object[] arr, int compTypeId) {
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
    public void writeArray(PortableWriterExImpl writer, byte elementType, Object[] arr, String clsName) {
        writer.writeByte(elementType);
        writer.writeInt(GridPortableMarshaller.UNREGISTERED_TYPE_ID);
        writer.writeString(clsName);
        writer.writeInt(arr.length);

        for (Object obj : arr)
            writeValue(writer, obj);
    }

}