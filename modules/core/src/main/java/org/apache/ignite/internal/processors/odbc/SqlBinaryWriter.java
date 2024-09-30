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
import java.sql.Blob;
import java.sql.SQLException;
import org.apache.ignite.internal.binary.BinaryContext;
import org.apache.ignite.internal.binary.BinaryWriterExImpl;
import org.apache.ignite.internal.binary.BinaryWriterHandles;
import org.apache.ignite.internal.binary.BinaryWriterSchemaHolder;
import org.apache.ignite.internal.binary.GridBinaryMarshaller;
import org.apache.ignite.internal.binary.streams.BinaryOutputStream;

/**
 * Binary writer for SQL.
 * <p>
 * Provides ability to write byte arrays from InputStreams and Blobs.
 */
public class SqlBinaryWriter extends BinaryWriterExImpl {
    /** Default buffer size. */
    public static final int DEFAULT_BUFFER_SIZE = 8192;

    /**
     * @param ctx Context.
     * @param out Output stream.
     * @param handles Handles.
     */
    public SqlBinaryWriter(BinaryContext ctx, BinaryOutputStream out, BinaryWriterSchemaHolder schema, BinaryWriterHandles handles) {
        super(ctx, out, schema, handles);
    }

    /**
     * Write byte array from the InputStream enclosed in the stream wrapper.
     *
     * @param inputStreamWrapper inputStreamWrapper
     */
    public void writeInputStreamAsByteArray(SqlInputStreamWrapper inputStreamWrapper) throws IOException {
        BinaryOutputStream out = out();

        InputStream in = inputStreamWrapper.getInputStream();
        int streamLength = inputStreamWrapper.getLength();

        if (in == null) {
            out.writeByte(GridBinaryMarshaller.NULL);

            return;
        }

        out.unsafeEnsure(1 + 4);
        out.unsafeWriteByte(GridBinaryMarshaller.BYTE_ARR);
        out.unsafeWriteInt(streamLength);

        out.unsafeEnsure(streamLength);
        int writtenLength = writeFromStream(in, out, streamLength);

        if (inputStreamWrapper.getLength() != writtenLength)
            throw new IOException("Input stream length mismatch. [declaredLength= " + inputStreamWrapper.getLength() + ", " +
                    "realLength= " + writtenLength + "]");
    }

    /**
     * Write byte array from the Blob instance.
     *
     * @param blob Blob.
     */
    public void writeBlobAsByteArray(Blob blob) throws SQLException, IOException {
        writeInputStreamAsByteArray(SqlInputStreamWrapper.withKnownLength(blob.getBinaryStream(1, blob.length()), (int)blob.length()));
    }

    /**
     * Copy data from the input stream to the binary output stream.
     *
     * <p>Copies no more than {@code limit} bytes.
     *
     * @param in Input stream.
     * @param out Output stream.
     * @param limit Maximum bytes to copy.
     * @return Count of bytes copied.
     */
    private int writeFromStream(InputStream in, BinaryOutputStream out, long limit) throws IOException {
        int writtenLen = 0;

        byte[] buf = new byte[DEFAULT_BUFFER_SIZE];

        while (writtenLen < limit) {
            int readLen = in.read(buf, 0, (int)Math.min(buf.length, limit - writtenLen));

            out.writeByteArray(buf, 0, readLen);

            writtenLen += readLen;
        }

        return writtenLen;
    }

    /**
     * @return Return underlying array.
     */
    public byte[] arrayUnderlying() {
        return out().array();
    }
}
