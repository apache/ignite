package org.apache.ignite.internal.binary.compress;

import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]> {
    public static ByteArrayComparator INSTANCE = new ByteArrayComparator();

    @Override public int compare(byte[] ba1, byte[] ba2) {
        return compare(ba1, 0, ba2, false);
    }

    public int compare(byte[] ba1, int offset, byte[] ba2, boolean startsWith) {
        int l1 = ba1.length;
        int l2 = ba2.length;
        for (int i = 0; ; i++) {
            if (l2 == i)
                return startsWith ? 0 : l1 - l2;
            else if (l1 <= i + offset)
                return l1 - l2;

            if (ba1[i + offset] != ba2[i])
                return ba1[i + offset] - ba2[i];
        }
    }
}
