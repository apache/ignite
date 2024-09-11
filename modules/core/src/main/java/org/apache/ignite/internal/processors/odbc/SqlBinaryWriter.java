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

package org.apache.ignite.internal.processors.odbc;

import java.io.IOException;
import java.io.InputStream;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.BinaryWriterHandles;
import org.apache.ignite.internal.binary.BinaryWriterSchemaHolder;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Binary writer for SQL.
 */
public class SqlBinaryWriter extends BinaryWriterExImpl {
    /** Default buffer size. */
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    /** */
    public SqlBinaryWriter(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema, BinaryWriterHandles handles) {
        super(ctx, out, schema, handles);
    }

    /**
     * Write byte array from the inputStream.
     * @param inputStreamWrapper inputStreamWrapper
     */
    public void writeInputStreamAsByteArray(SqlInputStreamWrapper inputStreamWrapper) throws IOException {
        BinaryOutputStream out = out();
        InputStream in = inputStreamWrapper.getStream();
        int wrapperLength = inputStreamWrapper.getLength();

        if (in == null) {
            out.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        int readLength;
        int writtenLength = 0;
        byte[] buf = new byte[DEFAULT_BUFFER_SIZE];

        out.unsafeEnsure(1 + 4);
        out.unsafeWriteByte(GridBinaryMarshaller.BYTE_ARR);
        out.unsafeWriteInt(wrapperLength);

        while (-1 != (readLength = in.read(buf)) && writtenLength < wrapperLength) {
            out.writeByteArray(buf, 0, readLength);

            writtenLength += readLength;
        }

        if (inputStreamWrapper.getLength() != writtenLength)
            throw new IOException("Input stream length mismatch. [wrapperLength= " + wrapperLength + ", " +
                    "writtenLength= " + writtenLength + "]");
    }
}
