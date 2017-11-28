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
import java.util.Arrays;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.OK;

public class DeflaterCompressEngine implements CompressEngine {
    /** */
    private boolean isInboundDone = false;

    /** */
    public CompressEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        byte[] bytes = new byte[src.remaining()];

        src.get(bytes);

        Deflater compressor = new Deflater();

        compressor.setLevel(Deflater.BEST_SPEED);
        compressor.setInput(bytes);
        compressor.finish();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] buf0 = new byte[32];

        while (!compressor.finished()) {
            int count = compressor.deflate(buf0);

            baos.write(buf0, 0, count);
        }

        compressor.end();
        baos.close();

        bytes = baos.toByteArray();

        bytes = concat(toArray(bytes.length), bytes);

        if (bytes.length > buf.remaining())
            return BUFFER_OVERFLOW;

        buf.put(bytes);

        return OK;
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    /** */
    public boolean isInboundDone() {
        return isInboundDone;
    }

    /** */
    public void closeInbound() throws IOException{
        //No-op
    }

    /** */
    public CompressEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        isInboundDone = false;

        if (src.remaining() == 0)
            return BUFFER_UNDERFLOW;

        int initPos = src.position();

        byte[] bytes;
        byte[] output = new byte[0];

        byte[] lenBytes = new byte[4];
        int len;

        while (!isInboundDone) {
            if (src.remaining() <= 5) {
                src.position(initPos);

                return BUFFER_UNDERFLOW;
            }

            src.get(lenBytes);

            len = toInt(lenBytes);

            if (src.remaining() < len) {
                src.position(initPos);

                return BUFFER_UNDERFLOW;
            }

            bytes = new byte[len]; //may be reuse or ByteArrayInputStream->ByteBufferInputStream

            src.get(bytes);

            Inflater inflater = new Inflater();
            inflater.setInput(bytes);

            ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length);

            byte[] buf0 = new byte[32];

            while (!inflater.finished()) {
                try {
                    int count = inflater.inflate(buf0);

                    baos.write(buf0, 0, count);

                } catch (DataFormatException e) {
                    throw new IOException("DataFormatException: ", e);
                }
            }

            inflater.end();

            baos.close();

            output = concat(output, baos.toByteArray());

            if (src.remaining() == 0)
                isInboundDone = true;
        }

        if (output.length > buf.remaining()) {
            src.position(initPos);

            return BUFFER_OVERFLOW;
        }

        buf.put(output);

        return OK;
    }

    /** */
    private int toInt(byte[] bytes){
        return ((bytes[0] & 0xFF) << 24) | ((bytes[1] & 0xFF) << 16)
            | ((bytes[2] & 0xFF) << 8) | (bytes[3] & 0xFF);
    }

    /** */
    private byte[] toArray(int val){
        return  new byte[] {
            (byte)(val >>> 24),
            (byte)(val >>> 16),
            (byte)(val >>> 8),
            (byte)val
        };
    }
}
