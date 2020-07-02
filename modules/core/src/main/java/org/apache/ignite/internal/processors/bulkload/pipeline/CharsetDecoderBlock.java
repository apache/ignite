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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.Arrays;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteIllegalStateException;

/**
 * A {@link PipelineBlock}, which converts stream of bytes supplied as byte[] arrays to an array of char[] using
 * the specified encoding. Decoding errors (malformed input and unmappable characters) are to handled by dropping
 * the erroneous input, appending the coder's replacement value to the output buffer, and resuming the coding operation.
 */
public class CharsetDecoderBlock extends PipelineBlock<byte[], char[]> {
    /** Empty portion. */
    public static final char[] EMPTY_PORTION = new char[0];

    /** Charset decoder */
    private final CharsetDecoder charsetDecoder;

    /** Leftover bytes (partial characters) from the last batch,
     * or null if everything was processed. */
    private byte[] leftover;

    /** True once we've reached the end of input. */
    private boolean isEndOfInput;

    /**
     * Creates charset decoder block.
     *
     * @param charset The charset encoding to decode bytes from.
     */
    public CharsetDecoderBlock(Charset charset) {
        charsetDecoder = charset.newDecoder()
            .onMalformedInput(CodingErrorAction.REPLACE)
            .onUnmappableCharacter(CodingErrorAction.REPLACE);

        isEndOfInput = false;
        leftover = null;
    }

    /** {@inheritDoc} */
    @Override public void accept(byte[] data, boolean isLastAppend) throws IgniteCheckedException {
        assert nextBlock != null;

        assert !isEndOfInput : "convertBytes() called after end of input";

        isEndOfInput = isLastAppend;

        if (leftover == null && data.length == 0) {
            nextBlock.accept(EMPTY_PORTION, isLastAppend);

            return;
        }

        ByteBuffer dataBuf;

        if (leftover == null)
            dataBuf = ByteBuffer.wrap(data);
        else {
            dataBuf = ByteBuffer.allocate(leftover.length + data.length);

            dataBuf.put(leftover).put(data);

            dataBuf.flip();

            leftover = null;
        }

        int outBufLen = (int)Math.ceil(charsetDecoder.maxCharsPerByte() * (data.length + 1));

        assert outBufLen > 0;

        CharBuffer outBuf = CharBuffer.allocate(outBufLen);

        for (;;) {
            CoderResult res = charsetDecoder.decode(dataBuf, outBuf, isEndOfInput);

            if (res.isUnderflow()) {
                // End of input buffer reached. Either skip the partial character at the end or wait for the next batch.
                if (!isEndOfInput && dataBuf.remaining() > 0)
                    leftover = Arrays.copyOfRange(dataBuf.array(),
                        dataBuf.arrayOffset() + dataBuf.position(), dataBuf.limit());

                // See {@link CharsetDecoder} class javadoc for the protocol.
                if (isEndOfInput)
                    charsetDecoder.flush(outBuf);

                if (outBuf.position() > 0)
                    nextBlock.accept(Arrays.copyOfRange(outBuf.array(), outBuf.arrayOffset(), outBuf.position()),
                        isEndOfInput);

                break;
            }

            // Not enough space in the output buffer, flush it and retry.
            if (res.isOverflow()) {
                assert outBuf.position() > 0;

                nextBlock.accept(Arrays.copyOfRange(outBuf.array(), outBuf.arrayOffset(), outBuf.position()),
                    isEndOfInput);

                outBuf.flip();

                continue;
            }

            assert !res.isMalformed() && !res.isUnmappable();

            // We're not supposed to reach this point with the current implementation.
            // The code below will fire exception if Oracle implementation of CharsetDecoder will be changed in future.
            throw new IgniteIllegalStateException("Unknown CharsetDecoder state");
        }
    }
}
