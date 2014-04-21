package bug;

import sun.misc.*;

import java.lang.reflect.*;
import java.util.*;

public class JVMFailure {
    private static final int KEY_SIZE_OFF = 0;
    private static final int VAL_SIZE_OFF = 4;
    private static final int HEADER_SIZE = 8;

    private static final Unsafe UNSAFE = unsafe();
    private static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    private static Unsafe unsafe() {
        try {
            return Unsafe.getUnsafe();
        }
        catch (Exception e) {
            try {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");

                field.setAccessible(true);

                return (Unsafe)field.get(null);
            }
            catch (Exception e1) {
                throw new RuntimeException(e1);
            }
        }
    }

    private static int readInt(long ptr) {
        return UNSAFE.getInt(ptr);
    }

    private static byte[] readBytes(long ptr, byte[] arr) {
        UNSAFE.copyMemory(null, ptr, arr, BYTE_ARR_OFF, arr.length);

        return arr;
    }

    private Deque<Long> ptrs = new ArrayDeque<>();

    private void put(byte[] keyBytes, byte[] valBytes) {
        long ptr = UNSAFE.allocateMemory(HEADER_SIZE + keyBytes.length + valBytes.length);

        UNSAFE.putInt(ptr + KEY_SIZE_OFF, keyBytes.length);
        UNSAFE.putInt(ptr + VAL_SIZE_OFF, valBytes.length);
        UNSAFE.copyMemory(keyBytes, BYTE_ARR_OFF, null, ptr + HEADER_SIZE, keyBytes.length);
        UNSAFE.copyMemory(valBytes, BYTE_ARR_OFF, null, ptr + HEADER_SIZE + keyBytes.length, valBytes.length);

        ptrs.addLast(ptr);

        if (ptrs.size() > 100) {
            long oldPtr = ptrs.pollFirst();

            byte[] valBytes0 = faultyMethod(oldPtr);

            if (valBytes0[0] != 0)
                System.out.println(oldPtr + " " + Arrays.toString(valBytes0));

            int keySize = UNSAFE.getInt(oldPtr + KEY_SIZE_OFF);
            int valSize = UNSAFE.getInt(oldPtr + VAL_SIZE_OFF);

            UNSAFE.setMemory(oldPtr, HEADER_SIZE + keySize + valSize, (byte)0xAB);
            UNSAFE.freeMemory(oldPtr);
        }
    }

    private byte[] faultyMethod(long ptr) {
        int keySize = readInt(ptr + KEY_SIZE_OFF);
        int valSize = readInt(ptr + VAL_SIZE_OFF);

        return readBytes(ptr + HEADER_SIZE + keySize, new byte[valSize]);
    }

    public static void main(String[] args) {
        JVMFailure fail = new JVMFailure();

        byte[] keyBytes = new byte[] { 127, 127, 127, 127 };

        byte[] valBytes = new byte[8];
        for (int i = 0; i < 8; i++)
            valBytes[i] = (byte)i;

        for (;;)
            fail.put(keyBytes, valBytes);
    }
}
