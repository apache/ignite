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

import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_OVERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.BUFFER_UNDERFLOW;
import static org.apache.ignite.internal.util.nio.compress.CompressEngineResult.OK;

public class DeflaterCompressEngine implements CompressEngine {
    /* For debug stats. */
    private long bytesBefore = 0;
    private long bytesAfter = 0;

    private final Deflater deflater = new Deflater();
    private final byte[] deflateArray = new byte[1024];
    private byte[] inputWrapArray = new byte[1024];
    private final ByteArrayOutputStream deflateBaos = new ByteArrayOutputStream(1024);

    private final Inflater inflater = new Inflater();
    private final byte[] inflateArray = new byte[1024];
    private byte[] inputUnwapArray = new byte[1024];
    private final ByteArrayOutputStream inflateBaos = new ByteArrayOutputStream(1024);


    public DeflaterCompressEngine(){
        deflater.setLevel(Deflater.BEST_SPEED);
    }

    /** */
    public CompressEngineResult wrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        bytesBefore += src.remaining();

        while (inputWrapArray.length < src.remaining())
            inputWrapArray = new byte[inputWrapArray.length * 2];

        int len = src.remaining();

        src.get(inputWrapArray, 0, len);

        deflater.reset();
        deflater.setInput(inputWrapArray, 0, len);
        deflater.finish();

        deflateBaos.reset();

        while (!deflater.finished()) {
            int count = deflater.deflate(deflateArray);

            deflateBaos.write(deflateArray, 0, count);
        }

        byte[] bytes = deflateBaos.toByteArray();

        bytesAfter += bytes.length;

        if (bytes.length > buf.remaining())
            return BUFFER_OVERFLOW;

        buf.put(bytes);

        return OK;
    }

    /** */
    public void closeInbound() throws IOException{
        //No-op
        System.out.println("MY bytesBefore:"+bytesBefore+" bytesAfter;"+bytesAfter+ " cr="+bytesBefore*1.0/bytesAfter);
    }

    /** */
    public CompressEngineResult unwrap(ByteBuffer src, ByteBuffer buf) throws IOException {
        if (src.remaining() == 0)
            return BUFFER_UNDERFLOW;

        while (inputUnwapArray.length < src.remaining())
            inputUnwapArray = new byte[inputUnwapArray.length * 2];

        int len = src.remaining();

        src.get(inputUnwapArray, 0, len);

        inflater.setInput(inputUnwapArray, 0, len);

        while (!inflater.finished() && !inflater.needsInput()) {
            try {
                int count = inflater.inflate(inflateArray);
                inflateBaos.write(inflateArray, 0, count);
            } catch (DataFormatException e) {
                throw new IOException("DataFormatException: ", e);
            }
        }

        if (inflater.finished()) {
            if (inflater.getRemaining() > 0)
                src.position(src.position() - inflater.getRemaining());

            inflater.reset();

            byte[] output = inflateBaos.toByteArray();
            inflateBaos.reset();

            if (buf.remaining() < output.length) {
                src.rewind();

                return BUFFER_OVERFLOW;
            }

            buf.put(output);

            if (src.remaining() == 0)
                return BUFFER_UNDERFLOW;
        }
       /* else
            return BUFFER_UNDERFLOW;*/

        return OK;
    }
}
