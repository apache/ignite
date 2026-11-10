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

package org.apache.ignite;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.Externalizable;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamClass;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import org.apache.ignite.marshaller.Marshaller;

/**
 * IO test utilities.
 */
public final class GridTestIoUtils {
    /**
     * Serializes a given object into byte array.
     *
     * @param obj Object to serialize.
     * @return Byte array containing serialized object.
     * @throws IOException If serialization failed.
     */
    public static byte[] serializeJdk(Object obj) throws IOException {
        ByteArrayOutputStream byteOut = null;

        ObjectOutputStream objOut = null;

        try {
            // Allocate half of kilobyte to avoid very small resizings.
            byteOut = new ByteArrayOutputStream(512);

            objOut = new ObjectOutputStream(byteOut);

            // Make sure that we serialize only task, without
            // class loader.
            objOut.writeObject(obj);

            objOut.flush();

            return byteOut.toByteArray();
        }
        finally {
            close(objOut);
            close(byteOut);
        }
    }

    /**
     * Deserializes passed in bytes using provided class loader.
     *
     * @param <T> Type of result object.
     * @param bytes Object bytes to deserialize.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialization failed.
     */
    public static <T extends Serializable> T deserializeJdk(byte[] bytes) throws IOException, ClassNotFoundException {
        ObjectInputStream in = null;

        try {
            in = new ObjectInputStream(new ByteArrayInputStream(bytes));

            return (T)in.readObject();
        }
        finally {
            close(in);
        }
    }

    /**
     * Deserializes passed in bytes using provided class loader.
     *
     * @param <T> Type of result object.
     * @param bytes Object bytes to deserialize.
     * @param clsLdr Class loader.
     * @return Deserialized object.
     * @throws IOException If deserialization failed.
     * @throws ClassNotFoundException If deserialization failed.
     */
    public static <T> T deserializeJdk(byte[] bytes, final ClassLoader clsLdr)
        throws IOException, ClassNotFoundException {
        ObjectInputStream in = null;

        try {
            in = new ObjectInputStream(new ByteArrayInputStream(bytes)) {
                @Override protected Class<?> resolveClass(ObjectStreamClass desc) throws ClassNotFoundException {
                    return clsLdr.loadClass(desc.getName());
                }
            };

            return (T)in.readObject();
        }
        finally {
            close(in);
        }
    }

    /**
     * @param <T> Type of result object.
     * @param obj Object to marshal/unmarshal.
     * @param marshaller Marshaller to use for serialization.
     * @return The same object, but passed through marshaller: obj->marshal->buf->unmarshal->copy.
     * @throws Exception If failed.
     */
    public static <T> T externalize(Externalizable obj, Marshaller marshaller) throws Exception {
        assert marshaller != null;

        byte[] buf = marshaller.marshal(obj);

        // Sleep to make sure that clock advances (necessary for some tests)
        Thread.sleep(10);

        return (T)marshaller.unmarshal(buf, obj.getClass().getClassLoader());
    }

    /**
     * Gets short value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static short getShortByByteLE(byte[] arr) {
        return getShortByByteLE(arr, 0);
    }

    /**
     * Gets short value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static short getShortByByteLE(byte[] arr, int off) {
        return (short)((arr[off] & 0xff) | arr[off + 1] << 8);
    }

    /**
     * Gets char value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static char getCharByByteLE(byte[] arr) {
        return getCharByByteLE(arr, 0);
    }

    /**
     * Gets char value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static char getCharByByteLE(byte[] arr, int off) {
        return (char)((arr[off] & 0xff) | arr[off + 1] << 8);
    }

    /**
     * Gets integer value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static int getIntByByteLE(byte[] arr) {
        return getIntByByteLE(arr, 0);
    }

    /**
     * Gets integer value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static int getIntByByteLE(byte[] arr, int off) {
        return ((int)arr[off] & 0xff) | ((int)arr[off + 1] & 0xff) << 8 |
            ((int)arr[off + 2] & 0xff) << 16 | ((int)arr[off + 3] & 0xff) << 24;
    }

    /**
     * Gets long value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static long getLongByByteLE(byte[] arr) {
        return getLongByByteLE(arr, 0);
    }

    /**
     * Gets long value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static long getLongByByteLE(byte[] arr, int off) {
        return ((long)arr[off] & 0xff) | ((long)arr[off + 1] & 0xff) << 8 | ((long)arr[off + 2] & 0xff) << 16 |
            ((long)arr[off + 3] & 0xff) << 24 | ((long)arr[off + 4] & 0xff) << 32 | ((long)arr[off + 5] & 0xff) << 40 |
            ((long)arr[off + 6] & 0xff) << 48 | ((long)arr[off + 7] & 0xff) << 56;
    }

    /**
     * Gets float value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static float getFloatByByteLE(byte[] arr) {
        return getFloatByByteLE(arr, 0);
    }

    /**
     * Gets float value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static float getFloatByByteLE(byte[] arr, int off) {
        return Float.intBitsToFloat(getIntByByteLE(arr, off));
    }

    /**
     * Gets double value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     */
    public static double getDoubleByByteLE(byte[] arr) {
        return getDoubleByByteLE(arr, 0);
    }

    /**
     * Gets double value from byte array assuming that value stored in little-endian byte order.
     *
     * @param arr Byte array.
     * @param off Offset.
     */
    public static double getDoubleByByteLE(byte[] arr, int off) {
        return Double.longBitsToDouble(getLongByByteLE(arr, off));
    }

    /**
     * Closes resource.
     *
     * @param c Resource
     * @throws IOException If failed to close resource.
     */
    private static void close(Closeable c) throws IOException {
        if (c != null)
            c.close();
    }

    /**
     * Ensure singleton.
     */
    private GridTestIoUtils() {
        // No-op.
    }

    /**
     * Reads whole stream content and returns it as a string.
     * Uses JDK-only implementation to avoid external dependencies.
     *
     * @param inputStream Stream to read.
     * @return Stream content as a string.
     * @throws IOException If something goes wrong.
     */
    public static String readStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] buf = new byte[4096];
        int len;

        while ((len = inputStream.read(buf)) != -1)
            baos.write(buf, 0, len);

        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }
}
