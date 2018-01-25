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

        if (data.length == 0) {
            nextBlock.accept(new char[0], isLastAppend);
            return;
        }

        ByteBuffer dataBuf = ByteBuffer.wrap(data);

        CharBuffer outBuf = CharBuffer.allocate(data.length + 2);

        for (; ; ) {
            CoderResult res = charsetDecoder.decode(dataBuf, outBuf, isEof);

            if (res.isUnderflow()) {
                if (isEof)
                    break;

                if (!dataBuf.hasRemaining())
                    break;

                if (dataBuf.position() > 0 && !isEof)
                    break;
            }
            else if (res.isOverflow()) {
                assert outBuf.position() > 0;

                break;
            }

            try {
                res.throwException();
            } catch (CharacterCodingException e) {
                throw new IgniteCheckedException(e);
            }
        }

        if (outBuf.position() == 0) {
            assert isEof;
            return;
        }

        nextBlock.accept(Arrays.copyOfRange(outBuf.array(), outBuf.arrayOffset(), outBuf.position()), isEof);
    }
}
