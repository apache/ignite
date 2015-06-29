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
        private ArrayList<Long> data;

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
        @Override public void addNextFieldOff(int off) {
            data.add((long)(off & ~FOOTER_BODY_IS_HANDLE_MASK));
        }

        /** {@inheritDoc} */
        @Override public void addNextHandleField(int handleOff, int handleLength) {
            hasHandles = true;

            data.add(((long)handleLength << 32) | (handleOff | FOOTER_BODY_IS_HANDLE_MASK));
        }

        /** {@inheritDoc} */
        @Override public void write() throws IOException {
            if (data == null)
                writeShort(EMPTY_FOOTER);
            else {
                // +5 - 2 bytes for footer len at the beginning, 2 bytes for footer len at the end, 1 byte for handles
                // indicator flag.
                short footerLen = (short)(data.size() * (hasHandles ? 8 : 4) + 5);

                writeShort(footerLen);

                if (hasHandles) {
                    for (long body : data)
                        writeLong(body);
                }
                else {
                    for (long body : data)
                        writeInt((int)body);
                }

                writeByte(hasHandles ? 1 : 0);

                writeShort(footerLen);
            }
        }
    }
}
