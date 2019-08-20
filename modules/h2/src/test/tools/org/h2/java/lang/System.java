/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.java.lang;

import java.io.PrintStream;

/**
 * A simple java.lang.System implementation.
 */
public class System {

    /**
     * The stdout stream.
     */
    public static PrintStream out;

    /**
     * Copy data from the source to the target.
     * Source and target may overlap.
     *
     * @param src the source array
     * @param srcPos the first element in the source array
     * @param dest the destination
     * @param destPos the first element in the destination
     * @param length the number of element to copy
     */
    public static void arraycopy(char[] src, int srcPos, char[] dest,
            int destPos, int length) {
        /* c:
        memmove(((jchar*)dest->getPointer()) + destPos,
            ((jchar*)src->getPointer()) + srcPos, sizeof(jchar) * length);
        */
        // c: return;
        java.lang.System.arraycopy(src, srcPos, dest, destPos, length);
    }

    /**
     * Copy data from the source to the target.
     * Source and target may overlap.
     *
     * @param src the source array
     * @param srcPos the first element in the source array
     * @param dest the destination
     * @param destPos the first element in the destination
     * @param length the number of element to copy
     */
    public static void arraycopy(byte[] src, int srcPos, byte[] dest,
            int destPos, int length) {
        /* c:
        memmove(((jbyte*)dest->getPointer()) + destPos,
            ((jbyte*)src->getPointer()) + srcPos, sizeof(jbyte) * length);
        */
        // c: return;
        java.lang.System.arraycopy(src, srcPos, dest, destPos, length);
    }

    /**
     * Get the current time in milliseconds since 1970-01-01.
     *
     * @return the milliseconds
     */
    public static long nanoTime() {
        /* c:
        #if CLOCKS_PER_SEC == 1000000
        return (jlong) clock() * 1000;
        #else
        return (jlong) clock() * 1000000 / CLOCKS_PER_SEC;
        #endif
        */
        // c: return;
        return java.lang.System.nanoTime();
    }

}
