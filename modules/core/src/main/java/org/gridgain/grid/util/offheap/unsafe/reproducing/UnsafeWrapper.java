// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util.offheap.unsafe.reproducing;

import sun.misc.*;
import java.lang.reflect.*;
import java.security.*;

public class UnsafeWrapper {
    private static final Unsafe UNSAFE = unsafe();

    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    public static int readInt(long ptr) {
        if (ptr == 0)
            Thread.yield();

        return UNSAFE.getInt(ptr);
    }

    public static byte[] readBytes(long ptr, byte[] arr) {
        UNSAFE.copyMemory(null, ptr, arr, BYTE_ARR_OFF, arr.length);

        return arr;
    }

    public static byte[] faultyMethod(long ptr) {
        int keyLen = UnsafeWrapper.readInt(ptr + 4);
        int valLen = UnsafeWrapper.readInt(ptr + 8);

        return UnsafeWrapper.readBytes(ptr + 28 + keyLen, new byte[valLen]);
    }

    /**
     * @return Instance of Unsafe class.
     */
    public static Unsafe unsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException ignored) {
            try {
                return AccessController.doPrivileged
                    (new PrivilegedExceptionAction<Unsafe>() {
                        @Override
                        public Unsafe run() throws Exception {
                            Field f = Unsafe.class.getDeclaredField("theUnsafe");

                            f.setAccessible(true);

                            return (Unsafe) f.get(null);
                        }
                    });
            }
            catch (PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics.", e.getCause());
            }
        }
    }
}
