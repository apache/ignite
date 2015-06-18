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
package org.apache.ignite.marshaller.optimized.ext;

import org.apache.ignite.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;

import java.io.*;
import java.util.concurrent.*;

import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.*;
import static org.apache.ignite.marshaller.optimized.ext.OptimizedMarshallerExt.*;


/**
 * TODO: IGNITE-950
 */
public class OptimizedObjectInputStreamExt extends OptimizedObjectInputStream {
    /** */
    private OptimizedMarshallerExtMetaHandler metaHandler;

    /** {@inheritDoc} */
    public OptimizedObjectInputStreamExt(GridDataInput in) throws IOException {
        super(in);
    }

    /**
     * @param clsMap Class descriptors by class map.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @param clsLdr Class loader.
     */
    protected void context(ConcurrentMap<Class, OptimizedClassDescriptor> clsMap, MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper, ClassLoader clsLdr, OptimizedMarshallerExtMetaHandler metaHandler) {
        context(clsMap, ctx, mapper, clsLdr);

        this.metaHandler = metaHandler;
    }

    /** {@inheritDoc} */
    @Override protected void skipFooter(Class<?> cls) throws IOException {
        if (metaHandler.metadata(resolveTypeId(cls.getName(), mapper)) != null) {
            int footerLen = in.readInt();

            if (footerLen != EMPTY_FOOTER)
                in.skipBytes(footerLen - 4);
        }
    }

    /** {@inheritDoc} */
    @Override protected int readFieldType() throws IOException {
        return in.readByte();
    }

    /**
     * Checks whether the object has a field with name {@code fieldName}.
     *
     * @param fieldName Field name.
     * @return {@code true} if field exists, {@code false} otherwise.
     * @throws IOException in case of error.
     */
    boolean hasField(String fieldName) throws IOException {
        int pos = in.position();

        if (in.readByte() != SERIALIZABLE) {
            in.position(pos);
            return false;
        }

        FieldRange range = fieldRange(fieldName, pos);

        in.position(pos);

        return range != null && range.start > 0;
    }

    /**
     * Looks up field with the given name and returns it in one of the following representations. If the field is
     * serializable and has a footer then it's not deserialized but rather returned wrapped by {@link CacheObjectImpl}
     * for future processing. In all other cases the field is fully deserialized.
     *
     * @param fieldName Field name.
     * @return Field.
     * @throws IOException In case of error.
     * @throws ClassNotFoundException In case of error.
     */
    <F> F readField(String fieldName) throws IOException, ClassNotFoundException {
        int pos = in.position();

        if (in.readByte() != SERIALIZABLE) {
            in.position(pos);
            return null;
        }

        FieldRange range = fieldRange(fieldName, pos);

        F field = null;

        if (range != null && range.start > 0) {
            in.position(range.start);

            if (in.readByte() == SERIALIZABLE && metaHandler.metadata(in.readInt()) != null)
                //Do we need to make a copy of array?
                field = (F)new CacheOptimizedObjectImpl(in.array(), range.start, range.len);
            else {
                in.position(range.start);
                field = (F)readObject();
            }
        }

        in.position(pos);

        return field;
    }

    /**
     * Returns field offset in the byte stream.
     *
     * @param fieldName Field name.
     * @param start Object's start offset.
     * @return positive range or {@code null} if the object doesn't have such a field.
     * @throws IOException in case of error.
     */
    private FieldRange fieldRange(String fieldName, int start) throws IOException {
        int fieldId = resolveFieldId(fieldName);

        int typeId = readInt();

        int clsNameLen = 0;

        if (typeId == 0) {
            int pos = in.position();

            typeId = OptimizedMarshallerUtils.resolveTypeId(readUTF(), mapper);

            clsNameLen = in.position() - pos;
        }

        OptimizedObjectMetadata meta = metaHandler.metadata(typeId);

        if (meta == null)
            // TODO: IGNITE-950 add warning!
            return null;

        int end = in.size();

        in.position(end - FOOTER_LEN_OFF);

        int footerLen = in.readInt();

        if (footerLen == EMPTY_FOOTER)
            return null;

        // reading 'hasHandles' flag. 1 byte - additional offset to get to the flag position.
        in.position(in.position() - FOOTER_LEN_OFF - 1);

        boolean hasHandles = in.readBoolean();

        // 4 - skipping length at the beginning
        int footerOff = (end - footerLen) + 4;
        in.position(footerOff);

        int fieldOff = 0;

        for (OptimizedObjectMetadata.FieldInfo info : meta.getMeta()) {
            if (info.id == fieldId) {
                int len = info.len == VARIABLE_LEN ? in.readInt() : info.len;
                int handlePos;

                if (hasHandles && info.len == VARIABLE_LEN)
                    handlePos = in.readInt();
                else
                    handlePos = NOT_A_HANDLE;

                if (handlePos == NOT_A_HANDLE) {
                    //object header len: 1 - for type, 4 - for type ID, 2 - for checksum.
                    fieldOff += 1 + 4 + clsNameLen + 2;

                    return new FieldRange(start + fieldOff, len);
                }
                else {
                    throw new IgniteException("UNSUPPORTED YET");
                }
            }
            else {
                fieldOff += info.len == VARIABLE_LEN ? in.readInt() : info.len;

                if (hasHandles) {
                    in.skipBytes(4);
                    fieldOff += 4;
                }

            }
        }

        return null;
    }

    /**
     *
     */
    private static class FieldRange {
        /** */
        private int start;

        /** */
        private int len;

        /**
         * @param start Start.
         * @param len   Length.
         */
        public FieldRange(int start, int len) {
            this.start = start;
            this.len = len;
        }
    }
}
