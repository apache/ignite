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

package org.apache.ignite.internal.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.ignite.lang.IgniteLogger;

/**
 * Utility class provides various method for manipulating with bytes.
 */
public class ByteUtils {
    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(ByteUtils.class);

    /**
     * Constructs {@code long} from byte array.
     *
     * @param bytes Array of bytes.
     * @param off Offset in {@code bytes} array.
     * @return Long value.
     */
    public static long bytesToLong(byte[] bytes, int off) {
        assert bytes != null;

        int bytesCnt = Long.SIZE >> 3;

        if (off + bytesCnt > bytes.length)
            bytesCnt = bytes.length - off;

        long res = 0;

        for (int i = 0; i < bytesCnt; i++) {
            int shift = bytesCnt - i - 1 << 3;

            res |= (0xffL & bytes[off++]) << shift;
        }

        return res;
    }

    /**
     * Converts primitive {@code long} type to byte array.
     *
     * @param l Long value.
     * @return Array of bytes.
     */
    public static byte[] longToBytes(long l) {
        return toBytes(l, new byte[8], 0, 8);
    }

    /**
     * Converts primitive {@code long} type to byte array and stores it in specified
     * byte array. The highest byte in the value is the first byte in result array.
     *
     * @param l Unsigned long value.
     * @param bytes Bytes array to write result to.
     * @param off Offset in the target array to write result to.
     * @param limit Limit of bytes to write into output.
     * @return Number of bytes overwritten in {@code bytes} array.
     */
    private static byte[] toBytes(long l, byte[] bytes, int off, int limit) {
        assert bytes != null;
        assert limit <= 8;
        assert bytes.length >= off + limit;

        for (int i = limit - 1; i >= 0; i--) {
            bytes[off + i] = (byte)(l & 0xFF);
            l >>>= 8;
        }

        return bytes;
    }

    /**
     * Serializes an object to byte array using native java serialization mechanism.
     *
     * @param obj Object to serialize.
     * @return Byte array.
     */
    public static byte[] toBytes(Object obj) {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
            try (ObjectOutputStream out = new ObjectOutputStream(bos)) {

                out.writeObject(obj);

                out.flush();

                return bos.toByteArray();
            }
        }
        catch (Exception e) {
            LOG.warn("Could not serialize a class [cls=" + obj.getClass().getName() + "]", e);

            return null;
        }
    }

    /**
     * Deserializes an object from byte array using native java serialization mechanism.
     *
     * @param bytes Byte array.
     * @return Object.
     */
    public static Object fromBytes(byte[] bytes) {
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes)) {
            try (ObjectInputStream in = new ObjectInputStream(bis)) {
                return in.readObject();
            }
        }
        catch (Exception e) {
            LOG.warn("Could not deserialize an object", e);

            return null;
        }
    }
}
