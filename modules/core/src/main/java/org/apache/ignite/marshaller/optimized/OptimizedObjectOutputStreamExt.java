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

package org.apache.ignite.marshaller.optimized;

import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.io.*;

import java.io.*;
import java.util.*;

import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerExt.*;
import static org.apache.ignite.marshaller.optimized.OptimizedMarshallerUtils.*;

/**
 * TODO: IGNITE-950
 */
public class OptimizedObjectOutputStreamExt extends OptimizedObjectOutputStream {
    /**
     * Constructor.
     *
     * @param out Output stream.
     * @throws IOException In case of error.
     */
    protected OptimizedObjectOutputStreamExt(GridDataOutput out) throws IOException {
        super(out);
    }

    /** {@inheritDoc} */
    @Override protected Footer createFooter(Class<?> cls) {
        if (fieldsIndexingSupported(cls, metaHandler, ctx, clsMap, mapper))
            return new FooterImpl();

        return null;
    }

    /** {@inheritDoc} */
    @Override protected void writeFieldType(byte type) throws IOException {
        out.writeByte(type);
    }

    /**
     *
     */
    private class FooterImpl implements OptimizedObjectOutputStream.Footer {
        /** */
        private ArrayList<Meta> data;

        /** */
        private HashMap<Integer, GridHandleTable.ObjectInfo> handles;

        /** */
        private boolean hasHandles;

        /** {@inheritDoc} */
        @Override public void indexingSupported(boolean indexingSupported) {
            if (indexingSupported)
                data = new ArrayList<>();
            else
                data = null;
        }

        /** {@inheritDoc} */
        @Override public void put(int fieldId, OptimizedFieldType fieldType, int len) {
            if (data == null)
                return;

            if (fieldType == OptimizedFieldType.OTHER)
                data.add(new Meta(fieldId, len));
        }

        /** {@inheritDoc} */
        @Override public void putHandle(int fieldId, GridHandleTable.ObjectInfo objInfo) {
            if (data == null)
                return;

            if (!hasHandles) {
                hasHandles = true;
                handles = new HashMap<>();
            }

            handles.put(fieldId, objInfo);

            // length of handle fields is 5 bytes.
            put(fieldId, OptimizedFieldType.OTHER, 5);
        }

        /** {@inheritDoc} */
        @Override public void write() throws IOException {
            if (data == null)
                writeShort(EMPTY_FOOTER);
            else {
                int bodyEnd = out.offset();

                // +4 - 2 bytes for footer len at the beginning, 2 bytes for footer len at the end.
                short footerLen = (short)(data.size() * 4 + 4);

                if (hasHandles)
                    footerLen += handles.size() * 8;

                writeShort(footerLen);

                if (hasHandles) {
                    for (int i = 0; i < data.size(); i++) {
                        Meta fieldMeta = data.get(i);

                        GridHandleTable.ObjectInfo objInfo = handles.get(fieldMeta.fieldId);

                        if (objInfo == null)
                            writeInt(fieldMeta.fieldLen & ~FOOTER_BODY_IS_HANDLE_MASK);
                        else {
                            writeInt(fieldMeta.fieldLen | FOOTER_BODY_IS_HANDLE_MASK);
                            writeInt(objInfo.position());

                            if (objInfo.length() == 0)
                                // field refers to its own object that hasn't set total length yet.
                                writeInt((bodyEnd - objInfo.position()) + footerLen);
                            else
                                writeInt(objInfo.length());
                        }
                    }
                }
                else
                    for (Meta fieldMeta : data)
                        // writing field len and resetting is handle mask
                        writeInt(fieldMeta.fieldLen & ~FOOTER_BODY_IS_HANDLE_MASK);

                writeShort(footerLen);
            }
        }
    }

    /**
     *
     */
    private static class Meta {
        /** */
        int fieldId;

        /** */
        int fieldLen;

        /**
         * @param fieldId
         * @param fieldLen
         */
        public Meta(int fieldId, int fieldLen) {
            this.fieldId = fieldId;
            this.fieldLen = fieldLen;
        }
    }
}
