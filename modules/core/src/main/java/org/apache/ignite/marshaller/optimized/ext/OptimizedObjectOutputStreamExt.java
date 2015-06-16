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

import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.marshaller.optimized.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.marshaller.optimized.ext.OptimizedMarshallerExt.*;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.*;

/**
 * TODO: IGNITE-950
 */
public class OptimizedObjectOutputStreamExt extends OptimizedObjectOutputStream {
    /** */
    private OptimizedMarshallerExtMetaHandler metaHandler;

    /** {@inheritDoc} */
    protected OptimizedObjectOutputStreamExt(GridDataOutput out) throws IOException {
        super(out);
    }

    /**
     * @param clsMap Class descriptors by class map.
     * @param ctx Context.
     * @param mapper ID mapper.
     * @param requireSer Require {@link Serializable} flag.
     * @param metaHandler Metadata handler.
     */
    protected void context(ConcurrentMap<Class, OptimizedClassDescriptor> clsMap, MarshallerContext ctx,
        OptimizedMarshallerIdMapper mapper, boolean requireSer, OptimizedMarshallerExtMetaHandler metaHandler) {
        context(clsMap, ctx, mapper, requireSer);

        this.metaHandler = metaHandler;
    }

    /** {@inheritDoc} */
    @Override protected void writeFieldType(byte type) throws IOException {
        out.writeByte(type);
    }

    /** {@inheritDoc} */
    @Override protected Footer createFooter(Class<?> cls) {
        if (metaHandler.metadata(resolveTypeId(cls.getName(), mapper)) != null)
            return new FooterImpl();
        else
            return null;
    }

    /**
     *
     */
    private class FooterImpl implements OptimizedObjectOutputStream.Footer {
        /** */
        private ArrayList<Short> data;

        /** */
        private int headerPos;

        /** {@inheritDoc} */
        @Override public void fields(OptimizedClassDescriptor.Fields fields) {
            if (fields.fieldsIndexingSupported())
                data = new ArrayList<>();
            else
                data = null;
        }

        /** {@inheritDoc} */
        public void headerPos(int pos) {
            headerPos = pos;
        }

        /** {@inheritDoc} */
        public void put(int fieldId, OptimizedFieldType fieldType, int len) {
            if (data == null)
                return;

            // Considering that field's length will be no longer 2^15 (32 MB)
            if (fieldType == OptimizedFieldType.OTHER)
                data.add((short)len);
        }

        /** {@inheritDoc} */
        @Override public void putHandle(int fieldId, int handleId) {
            disable();
        }

        /** {@inheritDoc} */
        public void write() throws IOException {
            if (data == null)
                writeInt(EMPTY_FOOTER);
            else {
                //12 - 4 bytes for len at the beginning, 4 bytes for len at the end, 4 bytes for object len.
                int footerLen = data.size() * 2 + 12;

                writeInt(footerLen);

                for (short fieldLen : data)
                    writeShort(fieldLen);

                // object total len
                writeInt((out.size() - headerPos) + 8);

                writeInt(footerLen);
            }
        }

        /**
         * Disable footer and indexing for the given Object.
         */
        private void disable() {
            data = null;
        }
    }
}
