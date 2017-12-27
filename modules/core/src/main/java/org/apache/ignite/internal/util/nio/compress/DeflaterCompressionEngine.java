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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressionEngineResult.OK;

public class DeflaterCompressionEngine implements CompressionEngine {
    /* For debug stats. */
    private long bytesBefore = 0;
    private long bytesAfter = 0;

    private final Deflater deflater = new Deflater();
    private final byte[] deflateArray = new byte[1024];
    private byte[] inputWrapArray = new byte[1024];
    private final ExtendedByteArrayOutputStream deflateBaos = new ExtendedByteArrayOutputStream(1024);

    private final Inflater inflater = new Inflater();
    private final byte[] inflateArray = new byte[1024];
    private byte[] inputUnwapArray = new byte[1024];
    private final ExtendedByteArrayOutputStream inflateBaos = new ExtendedByteArrayOutputStream(1024);


    public DeflaterCompressionEngine(){
        deflater.setLevel(Deflater.BEST_SPEED);
    }

    /** */
    public CompressionEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        int len = src.remaining();

        bytesBefore += len;

        while (inputWrapArray.length < len)
            inputWrapArray = new byte[inputWrapArray.length * 2];

        src.get(inputWrapArray, 0, len);

        deflater.reset();
        deflater.setInput(inputWrapArray, 0, len);
        deflater.finish();

        deflateBaos.reset();

        while (!deflater.finished()) {
            int count = deflater.deflate(deflateArray);

            deflateBaos.write(deflateArray, 0, count);
        }

        bytesAfter += deflateBaos.size();

        if (deflateBaos.size() > buf.remaining()) {
            src.rewind();

            return BUFFER_OVERFLOW;
        }

        buf.put(deflateBaos.getByteArray(), 0, deflateBaos.size());

        return OK;
    }

    /** */
    public void closeInbound() throws IOException{
        //No-op
        System.out.println("MY deflate bytesBefore:"+bytesBefore+" bytesAfter;"+bytesAfter+ " cr="+bytesBefore*1.0/bytesAfter);
    }

    private int inputUnwrapPos = 0;
    private int inputUnwapLen = 0;

    /** */
    public CompressionEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        if (inflateBaos.size() > 0){
            if (buf.remaining() < inflateBaos.size())
                return BUFFER_OVERFLOW;

            buf.put(inflateBaos.getByteArray(), 0, inflateBaos.size());

            inflateBaos.reset();
        }

        int len = src.remaining();

        while (inputUnwapArray.length < src.remaining())
            inputUnwapArray = new byte[inputUnwapArray.length * 2];

        if (inputUnwrapPos >= inputUnwapLen)
            if (len > 0) {
                src.get(inputUnwapArray, 0, len);

                inputUnwapLen = len;
                inputUnwrapPos = 0;
            } else
                return BUFFER_UNDERFLOW;

        inflater.setInput(inputUnwapArray, inputUnwrapPos, inputUnwapLen - inputUnwrapPos);

        while (!inflater.finished() && !inflater.needsInput()) {
            try {
                int count = inflater.inflate(inflateArray);
                inflateBaos.write(inflateArray, 0, count);
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
}
