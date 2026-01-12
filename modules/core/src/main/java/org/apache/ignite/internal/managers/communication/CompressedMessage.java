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

package org.apache.ignite.internal.managers.communication;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;
import org.apache.ignite.IgniteException;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/** TODO */
public class CompressedMessage implements Message {
    /** Type code. */
    public static final short TYPE_CODE = 517;

    /** */
    private static final int CHUNK_SIZE = 10 * 1024 * 1024;

    /** */
    private static final int BUFFER_CAPACITY = 100 * 1024;

    /** */
    private ByteBuffer tmpBuf;

    /** */
    private int dataSize = -1;

    /** */
    private ChunkedByteReader chunkedReader;

    /** */
    private byte[] chunk;

    /** */
    private boolean finalChunk;

    /** */
    public CompressedMessage() {
        // No-op.
    }

    /**
     * @param buf Source buffer with seralized data.
     */
    public CompressedMessage(ByteBuffer buf) {
        dataSize = buf.position();
        chunkedReader = new ChunkedByteReader(compress(buf), CHUNK_SIZE);
    }

    /** TODO */
    public static CompressedMessage empty() {
        CompressedMessage msg = new CompressedMessage();

        msg.dataSize = 0;
        msg.finalChunk = true;
        msg.chunk = null;

        return msg;
    }

    /** */
    public int dataSize() {
        return dataSize;
    }

    /** TODO */
    public byte[] uncompressed() {
        assert finalChunk;

        byte[] uncompress = uncompress();

        assert uncompress != null;
        assert uncompress.length == dataSize : "Expected=" + dataSize + ", actual=" + uncompress.length;

        return uncompress;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType()))
                return false;

            writer.onHeaderWritten();
        }

        while (true) {
            if (chunk == null && chunkedReader != null) {
                chunk = chunkedReader.nextChunk();

                System.out.println(">>> After next chunk [" + Arrays.toString(chunk) + ']');

                finalChunk = (chunk == null);
            }

            switch (writer.state()) {
                case 0:
                    if (!writer.writeInt(dataSize))
                        return false;

                    System.out.println(">>> WRITE dataSize=" + dataSize);

                    writer.incrementState();

                    if (dataSize == 0)
                        return true;

                case 1:
                    if (!writer.writeBoolean(finalChunk))
                        return false;

                    writer.incrementState();

                    if (finalChunk)
                        return true;

                case 2:
                    if (!writer.writeByteArray(chunk))
                        return false;

                    System.out.println(">>> WRITED chunk [length=" + chunk.length + ", chunk=" + Arrays.toString(chunk) + ']');

                    chunk = null;

                    writer.decrementState();
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (tmpBuf == null)
            tmpBuf = ByteBuffer.allocate(BUFFER_CAPACITY);

        assert chunk == null : chunk;

        while (true) {
            switch (reader.state()) {
                case 0:
                    dataSize = reader.readInt();

                    System.out.println(">>> READ dataSize=" + dataSize);

                    if (!reader.isLastRead())
                        return false;

                    if (dataSize == 0)
                        return true;

                    reader.incrementState();

                case 1:
                    finalChunk = reader.readBoolean();

                    if (!reader.isLastRead())
                        return false;

                    reader.incrementState();

                    if (finalChunk)
                        return true;

                case 2:
                    chunk = reader.readByteArray();

                    if (!reader.isLastRead())
                        return false;

                    if (chunk != null) {
                        tmpBuf.put(chunk);

                        System.out.println(">>> ADD chunk to tmpBuf [length=" + chunk.length + " , chunk=" + Arrays.toString(chunk) + ']');

                        reader.decrementState();

                        chunk = null;
                    }
            }
        }
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return TYPE_CODE;
    }

    /**
     * @param buf Buffer.
     */
    private byte[] compress(ByteBuffer buf) {
        byte[] data = new byte[buf.position()];

        buf.flip();
        buf.get(data);

        System.out.println(">>> RAW DATA: length=" + data.length + ", data=" + Arrays.toString(data));

        ByteArrayOutputStream baos = new ByteArrayOutputStream(data.length);
        Deflater deflater = new Deflater(Deflater.BEST_SPEED, true);

        try (DeflaterOutputStream dos = new DeflaterOutputStream(baos, deflater)) {
            dos.write(data);
            dos.finish();
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
        finally {
            deflater.end();
        }

        byte[] res = baos.toByteArray();

        System.out.println(">>> SUCCESS compress [length=" + res.length + ", res=" + Arrays.toString(res) + ']');

        return res;
    }

    /** */
    private byte[] uncompress() {
        if (tmpBuf == null)
            return null;

        byte[] uncompressedData;

        Inflater inflater = new Inflater(true);

        try (InflaterInputStream iis = new InflaterInputStream(new ByteArrayInputStream(tmpBuf.array()), inflater)) {
            uncompressedData = iis.readAllBytes();
        }
        catch (IOException ex) {
            throw new IgniteException(ex);
        }
        finally {
            inflater.end();
        }

        System.out.println(">>> SUCCESS uncompress [length=" + uncompressedData.length + ", data=" + Arrays.toString(uncompressedData) + ']');

        tmpBuf.clear();

        return uncompressedData;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "CompressedMessage{" +
                "chunk=" + Arrays.toString(chunk) +
                ", tmpBuf=" + tmpBuf +
                ", dataSize=" + dataSize +
                ", chunkedReader=" + chunkedReader +
                ", finalChunk=" + finalChunk +
                '}';
    }
}
