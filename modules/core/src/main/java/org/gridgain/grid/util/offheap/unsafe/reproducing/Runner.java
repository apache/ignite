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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public class Runner {

    private static Unsafe UNSAFE = unsafe();

    private static long ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    public static void main(String[] args) {
        // Prepare data.
        final int cnt = 1000;

        final List<Long> ptrs = createData(1000);

        // Force readInt compilation.
        compileReadInt();

        // Force readBytes compilation.
        compileReadBytes();

        // Call faulty method multiple times.
        final AtomicBoolean startFlag = new AtomicBoolean();

        for (int i = 0; i < 100; i++) {
            new Thread(new Runnable() {
                @Override public void run() {
                    while (!startFlag.get()) {
                        // Spin.
                    }

                    for (;;) {
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();

                        long nextPtr = ptrs.get(rnd.nextInt(0, cnt));

                        byte[] res = UnsafeWrapper.faultyMethod(nextPtr);

                        for (int i = 0; i < res.length; i++) {
                            if (res[i] != 2)
                                System.out.println("ERROR: " + Arrays.toString(res));
                        }
                    }
                }
            }).start();
        }

        startFlag.set(true);
    }

    private static void compileReadInt() {
        long ptr = UNSAFE.allocateMemory(4);

        long sum = 0;

        for (int i = 0; i < 1000000; i++) {
             sum += UnsafeWrapper.readInt(ptr);
        }

        System.out.println(sum);
    }

    private static void compileReadBytes() {
        long ptr = UNSAFE.allocateMemory(8);

        long sum = 0;

        for (int i = 0; i < 1000000; i++)
            UnsafeWrapper.readBytes(ptr, new byte[8]);
    }

    private static List<Long> createData(int cnt) {
        List<Long> ptrs = new ArrayList<>(cnt);

        for (int i = 0; i < cnt; i++) {
            byte[] keyArr = new byte[10];
            for (int j = 0; j < 10; j++)
                keyArr[j] = 1;

            byte[] valArr = new byte[160];
            for (int j = 0; j < 160; j++)
                valArr[j] = 2;

            long ptr = UNSAFE.allocateMemory(28 + 10 + 160);

            UNSAFE.putInt(ptr + 4, 10);
            UNSAFE.putInt(ptr + 8, 160);

            UNSAFE.copyMemory(keyArr, ARR_OFF, null, ptr + 28, keyArr.length);
            UNSAFE.copyMemory(valArr, ARR_OFF, null, ptr + 28 + 10, valArr.length);

            ptrs.add(ptr);
        }

        return ptrs;
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
                        @Override public Unsafe run() throws Exception {
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
