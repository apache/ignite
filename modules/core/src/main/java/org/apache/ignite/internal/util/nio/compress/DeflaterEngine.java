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

package org.apache.ignite.internal.util.nio.compress;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.OK;

/**
 * Implementation of Deflater algorithm.
 */
public class DeflaterEngine implements CompressionEngine {
    /** */
    private final Deflater deflater = new Deflater();

    /** */
    private byte[] inputDeflaterArr = new byte[32768];

    /** */
    private final ExtendedByteArrayOutputStream deflateBaos = new ExtendedByteArrayOutputStream(32768);

    /** */
    private final Inflater inflater = new Inflater();

    /** */
    private byte[] inputInflaterArr = new byte[32768];

    /** */
    private final ExtendedByteArrayOutputStream inflateBaos = new ExtendedByteArrayOutputStream(32768);

    /** */
    private int inputUnwrapPos = 0;

    /** */
    private int inputUnwapLen = 0;

    /** */
    public DeflaterEngine(){
        deflater.setLevel(Deflater.BEST_SPEED);
    }

    /** {@inheritDoc} */
    public CompressionEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        int len = src.remaining();

        while (inputDeflaterArr.length < len)
            inputDeflaterArr = new byte[inputDeflaterArr.length * 2];

        src.get(inputDeflaterArr, 0, len);

        deflater.reset();
        deflater.setInput(inputDeflaterArr, 0, len);
        deflater.finish();

        deflateBaos.reset();

        byte[] arr = new byte[1024];

        while (!deflater.finished()) {
            int count = deflater.deflate(arr);

            deflateBaos.write(arr, 0, count);
        }

        if (deflateBaos.size() > buf.remaining()) {
            src.rewind();

            return BUFFER_OVERFLOW;
        }

        buf.put(deflateBaos.getByteArray(), 0, deflateBaos.size());

        return OK;
    }

    /** {@inheritDoc} */
    public CompressionEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        if (inflateBaos.size() > 0){
            if (buf.remaining() < inflateBaos.size())
                return BUFFER_OVERFLOW;

            buf.put(inflateBaos.getByteArray(), 0, inflateBaos.size());

            inflateBaos.reset();
        }

        int len = src.remaining();

        while (inputInflaterArr.length < src.remaining())
            inputInflaterArr = new byte[inputInflaterArr.length * 2];

        if (inputUnwrapPos >= inputUnwapLen) {
            if (len > 0) {
                src.get(inputInflaterArr, 0, len);

                inputUnwapLen = len;
                inputUnwrapPos = 0;
            }
            else
                return BUFFER_UNDERFLOW;
        }

        inflater.setInput(inputInflaterArr, inputUnwrapPos, inputUnwapLen - inputUnwrapPos);

        byte[] arr = new byte[1024];

        while (!inflater.finished() && !inflater.needsInput()) {
            try {
                int count = inflater.inflate(arr);
                inflateBaos.write(arr, 0, count);
            } catch (DataFormatException e) {
                throw new IOException("DataFormatException: ", e);
            }
        }

        int readed = inflater.getRemaining();

        inputUnwrapPos =  inputUnwapLen - readed ;

        if (inflater.finished()) {
            inflater.reset();

            if (buf.remaining() < inflateBaos.size())
                return BUFFER_OVERFLOW;

            buf.put(inflateBaos.getByteArray(), 0, inflateBaos.size());

            inflateBaos.reset();

            if (src.remaining() == 0 && inputUnwrapPos == inputUnwapLen)
                return BUFFER_UNDERFLOW;
        }
        else
            return BUFFER_UNDERFLOW;

        return OK;
    }

    /** */
    class ExtendedByteArrayOutputStream extends ByteArrayOutputStream {
        /** */
        ExtendedByteArrayOutputStream(int size) {
            super(size);
        }

        /** */
        byte[] getByteArray() {
            return buf;
        }
    }
}
