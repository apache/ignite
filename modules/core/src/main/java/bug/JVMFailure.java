package bug;

import sun.misc.*;

import java.lang.reflect.*;
import java.util.*;

public class JVMFailure {
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
        long ptr = UNSAFE.allocateMemory(12 + keyBytes.length + valBytes.length);

        UNSAFE.putInt(ptr + 4, keyBytes.length);
        UNSAFE.putInt(ptr + 8, valBytes.length);
        UNSAFE.copyMemory(keyBytes, BYTE_ARR_OFF, null, ptr + 12, keyBytes.length);
        UNSAFE.copyMemory(valBytes, BYTE_ARR_OFF, null, ptr + 12 + keyBytes.length, valBytes.length);

        ptrs.addLast(ptr);

        if (ptrs.size() > 100) {
            long ptr0 = ptrs.pollFirst();

            byte[] valBytes0 = faultyMethod(ptr0);

            if (valBytes0[0] != 0)
                System.out.println(Arrays.toString(valBytes0));

            int keySize = UNSAFE.getInt(ptr + 4);
            int valSize = UNSAFE.getInt(ptr + 8);

            UNSAFE.setMemory(ptr, 12 + keySize + valSize, (byte)0xAB);
            UNSAFE.freeMemory(ptr);
        }
    }

    private byte[] faultyMethod(long ptr) {
        int keySize = readInt(ptr + 4);
        int valSize = readInt(ptr + 8);

        return readBytes(ptr + 12 + keySize, new byte[valSize]);
    }

    public static void main(String[] args) {
        JVMFailure fail = new JVMFailure();

        byte[] keyBytes = new byte[] { 127, 127, 127, 127 };

        byte[] valBytes = new byte[74];
        for (int i = 0; i < 74; i++)
            valBytes[i] = (byte)i;

        for (;;)
            fail.put(keyBytes, valBytes);
    }
}
