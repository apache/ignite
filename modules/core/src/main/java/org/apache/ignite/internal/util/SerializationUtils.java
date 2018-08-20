package org.apache.ignite.internal.util;

import java.io.*;

public class SerializationUtils {

    /**
     * Serializes object obj.
     *
     * @param obj the object to serialize.
     * @return the serialized array.
     */
    public static byte[] serialize(Object obj) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(512);
        ObjectOutputStream oos = null;
        try {
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                if (oos != null) {
                    oos.close();
                }
            } catch (IOException ex) {}
        }
        return baos.toByteArray();
    }

    /**
     * Deserializes object array.
     *
     * @param objectArray the byte array of an object.
     * @return the deserialized object.
     */
    public static Object deserialize(byte[] objectArray) {

        if (objectArray == null) {
            throw new IllegalArgumentException("The object array must not be null.");
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(objectArray);

        if (bais == null) {
            throw new IllegalArgumentException("The InputStream must not be null.");
        }

        ObjectInputStream ois = null;
        try {
            ois = new ObjectInputStream(bais);
            return ois.readObject();
        } catch (ClassNotFoundException ex) {
            throw new RuntimeException(ex);
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        } finally {
            try {
                if (ois != null) {
                    ois.close();
                }
            } catch (IOException ex) {}
        }

    }

    public static byte[] longToBytes(long l) {
        byte[] r = new byte[8];
        for (int i = 7; i >= 0; i--) {
            r[i] = (byte)(l & 0xFF);
            l >>= 8;
        }
        return r;
    }

    public static long bytesToLong(byte[] b) {
        long r = 0;
        for (int i = 0; i < 8; i++) {
            r <<= 8;
            r |= (b[i] & 0xFF);
        }
        return r;
    }

}
