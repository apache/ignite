/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkore.ignite.lucene.util;

import java.nio.ByteBuffer;

import org.apache.lucene.util.BytesRef;

/**
 * Utility class with some [[ByteBuffer]] transformation utilities.
 *
 * @author Andres de la Pena `adelapena@stratio.com`
 */

public class ByteBufferUtils {

    /**
     * Returns the specified [[ByteBuffer]] as a byte array.
     *
     * @param bb
     *            a [[ByteBuffer]] to be converted to a byte array
     * @return the byte array representation of `bb`
     */
    public static byte[] asArray(ByteBuffer bb) {
        return ByteBufferUtil.getArray(bb);
    }

    /**
     * Returns `true` if the specified [[ByteBuffer]] is empty, `false`
     * otherwise.
     *
     * @param byteBuffer
     *            the byte buffer
     * @return `true` if the specified [[ByteBuffer]] is empty, `false`
     *         otherwise.
     */
    public static boolean isEmpty(ByteBuffer bb) {
        return bb.remaining() == 0;
    }

    // /** Returns the [[ByteBuffer]]s contained in the specified byte buffer
    // according to the specified
    // * type.
    // *
    // * @param byteBuffer the byte buffer to be split
    // * @param type the type of the byte buffer
    // * @return the byte buffers contained in `byteBuffer` according to `type`
    // */
    // public static split(byteBuffer: ByteBuffer, `type`: AbstractType[_]):
    // Array[ByteBuffer] = `type` match {
    // case c: CompositeType => c.split(byteBuffer)
    // case _ => Array[ByteBuffer](byteBuffer)
    // }

    /**
     * Returns the hexadecimal [[String]] representation of the specified
     * [[ByteBuffer]].
     *
     * @param byteBuffer
     *            a [[ByteBuffer]]
     * @return the hexadecimal `string` representation of `byteBuffer`
     */
    public static String toHex(ByteBuffer byteBuffer) {
        if (byteBuffer == null)
            return null;
        return ByteBufferUtil.bytesToHex(byteBuffer);
    }

    /**
     * Returns the hexadecimal [[String]] representation of the specified
     * [[BytesRef]].
     *
     * @param bytesRef
     *            a [[BytesRef]]
     * @return the hexadecimal `String` representation of `bytesRef`
     */
    public static String toHex(BytesRef bytesRef) {
        return ByteBufferUtil.bytesToHex(byteBuffer(bytesRef));
    }

    /**
     * Returns the hexadecimal [[String]] representation of the specified
     * [[Byte]]s.
     *
     * @param bytes
     *            the bytes
     * @return The hexadecimal `String` representation of `bytes`
     */
    public static String toHex(Byte[] bytes) {
        return toHex(bytes);
    }

    /**
     * Returns the hexadecimal [[String]] representation of the specified
     * [[Byte]] array.
     *
     * @param bytes
     *            the byte array
     * @return The hexadecimal `String` representation of `bytes`
     */
    public static String toHex(byte[] bytes) {
        return Hex.bytesToHex(bytes, 0, bytes.length);
    }

    /**
     * Returns the hexadecimal [[String]] representation of the specified
     * [[Byte]].
     *
     * @param b
     *            the byte
     * @return the hexadecimal `String` representation of `b`
     */
    public static String toHex(Byte b) {
        return Hex.bytesToHex(b);
    }

    /**
     * Returns the [[BytesRef]] representation of the specified [[ByteBuffer]].
     *
     * @param bb
     *            the byte buffer
     * @return the [[BytesRef]] representation of the byte buffer
     */
    public static BytesRef bytesRef(ByteBuffer bb) {
        return new BytesRef(asArray(bb));
    }

    /**
     * Returns the [[ByteBuffer]] representation of the specified [[BytesRef]].
     *
     * @param bytesRef
     *            the [[BytesRef]]
     * @return the [[ByteBuffer]] representation of `bytesRef`
     */
    public static ByteBuffer byteBuffer(BytesRef bytesRef) {
        return ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.offset + bytesRef.length);
    }

    /**
     * Returns the [[ByteBuffer]] representation of the specified hex
     * [[String]].
     *
     * @param hex
     *            an hexadecimal representation of a byte array
     * @return the [[ByteBuffer]] representation of `hex`
     */
    public static ByteBuffer byteBuffer(String hex) {
        if (hex == null)
            return null;
        return ByteBufferUtil.hexToBytes(hex);
    }

    /**
     * Returns a [[ByteBuffer]] representing the specified array of
     * [[ByteBuffer]]s.
     *
     * @param bbs
     *            an array of byte buffers
     * @return a [[ByteBuffer]] representing `bbs`
     */

    public static ByteBuffer compose(ByteBuffer[] bbs) {
        int totalLength = 0;
        for (ByteBuffer d : bbs) {
            totalLength += d.remaining();
        }
        ByteBuffer out = ByteBuffer.allocate(totalLength);
        ByteBufferUtil.writeShortLength(out, bbs.length);
        for (ByteBuffer bb : bbs) {
            ByteBufferUtil.writeShortLength(out, bb.remaining());
            out.put(bb.duplicate());
        }
        out.flip();
        return out;
    }

    /**
     * Returns the components of the specified [[ByteBuffer]] created with
     * [[compose()]].
     *
     * @param bb
     *            a byte buffer created with [[compose()]]
     * @return the components of `bb`
     */
    public static ByteBuffer[] decompose(ByteBuffer bb) {
        ByteBuffer duplicate = bb.duplicate();
        int numComponents = ByteBufferUtil.readShortLength(duplicate);
        ByteBuffer[] out = new ByteBuffer[numComponents];
        for (int i = 0; i < numComponents; i++) {
            int componentLength = ByteBufferUtil.readShortLength(duplicate);
            out[i] = ByteBufferUtil.readBytes(duplicate, componentLength);
        }
        return out;
    }
}
