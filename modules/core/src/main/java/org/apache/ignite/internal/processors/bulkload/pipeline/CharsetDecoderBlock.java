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

/** FIXME SHQ */
public class CharsetDecoderBlock extends PipelineBlock<byte[], char[]> {

    private final CharsetDecoder charsetDecoder;

    private boolean isEof;

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
