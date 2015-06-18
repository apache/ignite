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
        private ArrayList<Integer> data;

        /** */
        private ArrayList<Integer> fields;

        /** */
        private HashMap<Integer, Integer> handles;

        /** */
        private boolean hasHandles;

        /** {@inheritDoc} */
        @Override public void fields(OptimizedClassDescriptor.Fields fields) {
            if (fields.fieldsIndexingSupported()) {
                data = new ArrayList<>();
                this.fields = new ArrayList<>();
            }
            else
                data = null;
        }

        /** {@inheritDoc} */
        public void put(int fieldId, OptimizedFieldType fieldType, int len) {
            if (data == null)
                return;

            if (fieldType == OptimizedFieldType.OTHER) {
                data.add(len);
                fields.add(fieldId);
            }
        }

        /** {@inheritDoc} */
        @Override public void putHandle(int fieldId, int handlePos) {
            if (data == null)
                return;

            if (!hasHandles) {
                hasHandles = true;
                handles = new HashMap<>();
            }

            handles.put(fieldId, handlePos);

            // length of handle fields is 5 bytes.
            put(fieldId, OptimizedFieldType.OTHER, 5);
        }

        /** {@inheritDoc} */
        public void write() throws IOException {
            if (data == null)
                writeInt(EMPTY_FOOTER);
            else {
                //9 - 4 bytes for len at the beginning, 4 bytes for len at the end, 1 byte for 'hasHandles' flag
                int footerLen = data.size() * 4 + 9;

                if (hasHandles)
                    footerLen += data.size() * 4;

                writeInt(footerLen);

                if (hasHandles) {
                    for (int i = 0; i < data.size(); i++) {
                        writeInt(data.get(i));

                        Integer handlePos = handles.get(fields.get(i));

                        writeInt(handlePos == null ? NOT_A_HANDLE : handlePos);
                    }
                }
                else {
                    for (int fieldLen : data)
                        writeInt(fieldLen);
                }

                writeBoolean(hasHandles);

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
