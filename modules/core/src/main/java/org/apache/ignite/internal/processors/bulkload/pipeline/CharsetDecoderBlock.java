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

package org.apache.ignite.internal.processors.bulkload.pipeline;

import org.apache.ignite.IgniteCheckedException;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;

/**
 * A {@link PipelineBlock}, which converts stream of bytes supplied as byte[] arrays to an array of char[] using
 * the specified encoding. Decoding errors (malformed input and unmappable characters) are to handled by dropping
 * the erroneous input, appending the coder's replacement value to the output buffer, and resuming the coding operation.
 */
public class CharsetDecoderBlock extends PipelineBlock<byte[], char[]> {

    /** Charset decoder */
    private final CharsetDecoder charsetDecoder;

    /** Leftover bytes (partial characters) from the last batch,
     * or null if everything was processed. */
    private byte[] leftover;

    /** True if we've met the end of input. */
    private boolean isEof;

    /**
     * Creates charset decoder block.
     *
     * @param charset The charset encoding to decode bytes from.
     */
    public CharsetDecoderBlock(Charset charset) {
        super();

        this.charsetDecoder = charset.newDecoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);

        isEof = false;

        leftover = null;
    }

    /**
     * Returns the eof.
     *
     * @return eof.
     */
    public boolean isEof() {
        return isEof;
    }

    /** {@inheritDoc} */
    public void accept(byte[] data, boolean isLastAppend) throws IgniteCheckedException {

        assert !isEof : "convertBytes() called after EOF";

        isEof = isLastAppend;

        if (leftover == null && data.length == 0) {
            nextBlock.accept(new char[0], isLastAppend);
            return;
        }

        ByteBuffer dataBuf;

        if (leftover == null)
            dataBuf = ByteBuffer.wrap(data);
        else {
            dataBuf = ByteBuffer.allocate(leftover.length + data.length);

            dataBuf.put(leftover)
                   .put(data);

            dataBuf.flip();

            leftover = null;
        }

        CharBuffer outBuf = CharBuffer.allocate((int) Math.ceil(charsetDecoder.maxCharsPerByte() * (data.length + 1)));

        for (;;) {
            CoderResult res = charsetDecoder.decode(dataBuf, outBuf, isEof);

            if (!isEof && outBuf.position() > 0) {
                nextBlock.accept(Arrays.copyOfRange(outBuf.array(), outBuf.arrayOffset(), outBuf.position()), isEof);
                outBuf.flip();
            }

            if (res.isUnderflow()) { // Skip the partial character at the end or wait for the next batch
                if (!isEof && dataBuf.remaining() > 0)
                    leftover = Arrays.copyOfRange(dataBuf.array(),
                        dataBuf.arrayOffset() + dataBuf.position(), dataBuf.limit());

                break;
            }

            if (res.isOverflow())
                continue;

            assert ! res.isMalformed() && ! res.isUnmappable();

            if (res.isError()) {
                try {
                    res.throwException();
                }
                catch (CharacterCodingException e) {
                    throw new IgniteCheckedException(e);
                }
            }

            assert false : "Unknown CharsetDecoder state";
        }
    }
}
