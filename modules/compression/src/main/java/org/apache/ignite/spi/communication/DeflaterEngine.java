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

package org.apache.ignite.spi.communication;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.ignite.internal.util.nio.compression.CompressionEngine;
import org.apache.ignite.internal.util.nio.compression.CompressionEngineResult;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compression.CompressionEngineResult.OK;

/**
 * Implementation of Deflater algorithm.
 */
public final class DeflaterEngine implements CompressionEngine {
    /** */
    private static final int INIT_ARR_SIZE = 1 << 15;

    /** */
    private static final int BATCH_SIZE = 1 << 10;

    /** */
    private final Deflater deflater = new Deflater();

    /** */
    private byte[] inputDeflaterArr = new byte[INIT_ARR_SIZE];

    /** */
    private final ExtendedByteArrayOutputStream deflateBaos = new ExtendedByteArrayOutputStream(INIT_ARR_SIZE);

    /** */
    private final Inflater inflater = new Inflater();

    /** */
    private byte[] inputInflaterArr = new byte[INIT_ARR_SIZE];

    /** */
    private final ExtendedByteArrayOutputStream inflateBaos = new ExtendedByteArrayOutputStream(INIT_ARR_SIZE);

    /** */
    private int inputUnwrapPos = 0;

    /** */
    private int inputUnwrapLen = 0;

    /** */
    public DeflaterEngine(){
        deflater.setLevel(Deflater.BEST_SPEED);
    }

    /** {@inheritDoc} */
    @Override public CompressionEngineResult compress(ByteBuffer src, ByteBuffer buf) {
        assert src != null;
        assert buf != null;

        int len = src.remaining();

        if (inputDeflaterArr.length < len)
            inputDeflaterArr = new byte[U.ceilPow2(len)];

        src.get(inputDeflaterArr, 0, len);

        deflater.reset();

        deflater.setInput(inputDeflaterArr, 0, len);

        deflater.finish();

        deflateBaos.reset();

        byte[] arr = new byte[BATCH_SIZE];

        while (!deflater.finished())
            deflateBaos.write(arr, 0, deflater.deflate(arr));

        if (deflateBaos.size() > buf.remaining()) {
            src.rewind();

            return BUFFER_OVERFLOW;
        }

        buf.put(deflateBaos.getByteArray(), 0, deflateBaos.size());

        return OK;
    }

    /** {@inheritDoc} */
    @Override public CompressionEngineResult decompress(ByteBuffer src, ByteBuffer buf) throws IOException {
        assert src != null;
        assert buf != null;

        if (inflateBaos.size() > 0){
            if (buf.remaining() < inflateBaos.size())
                return BUFFER_OVERFLOW;

            buf.put(inflateBaos.getByteArray(), 0, inflateBaos.size());

            inflateBaos.reset();
        }

        int len = src.remaining();

        while (inputInflaterArr.length < src.remaining()) {
            assert inputInflaterArr.length <= Integer.MAX_VALUE / 2;

            inputInflaterArr = new byte[inputInflaterArr.length * 2];
        }

        if (inputUnwrapPos >= inputUnwrapLen) {
            if (len > 0) {
                src.get(inputInflaterArr, 0, len);

                inputUnwrapLen = len;
                inputUnwrapPos = 0;
            }
            else
                return BUFFER_UNDERFLOW;
        }

        inflater.setInput(inputInflaterArr, inputUnwrapPos, inputUnwrapLen - inputUnwrapPos);

        byte[] arr = new byte[BATCH_SIZE];

        while (!inflater.finished() && !inflater.needsInput()) {
            try {
                inflateBaos.write(arr, 0, inflater.inflate(arr));
            } catch (DataFormatException e) {
                throw new IOException("Failed to decompress data: ", e);
            }
        }

        int readed = inflater.getRemaining();

        inputUnwrapPos =  inputUnwrapLen - readed ;

        if (inflater.finished()) {
            inflater.reset();

            if (buf.remaining() < inflateBaos.size())
                return BUFFER_OVERFLOW;

            buf.put(inflateBaos.getByteArray(), 0, inflateBaos.size());

            inflateBaos.reset();

            if (src.remaining() == 0 && inputUnwrapPos == inputUnwrapLen)
                return BUFFER_UNDERFLOW;
        }
        else
            return BUFFER_UNDERFLOW;

        return OK;
    }

    /**
     *  ByteArrayOutputStream extended by method for access to internal array.
     */
    static final class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {
        /** {@inheritDoc} */
        ExtendedByteArrayOutputStream(int size) {
            super(size);
        }

        /**
         * Get internal byte array to avoid copy data. See {@link #toByteArray()} for details.
         *
         * @return Internal byte array.
         */
        byte[] getByteArray() {
            return buf;
        }
    }
}
