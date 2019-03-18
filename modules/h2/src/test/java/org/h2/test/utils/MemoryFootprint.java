/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.utils;

import java.lang.instrument.Instrumentation;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.h2.engine.Constants;
import org.h2.result.RowImpl;
import org.h2.store.Data;
import org.h2.util.Profiler;
import org.h2.value.Value;

/**
 * Calculate the memory footprint of various objects.
 */
public class MemoryFootprint {

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) {
        // System.getProperties().store(System.out, "");
        print("Object", new Object());
        print("Timestamp", new java.sql.Timestamp(0));
        print("Date", new java.sql.Date(0));
        print("Time", new java.sql.Time(0));
        print("BigDecimal", new BigDecimal("0"));
        print("BigInteger", new BigInteger("0"));
        print("String", new String("Hello"));
        print("Data", Data.create(null, 10));
        print("Row", new RowImpl(new Value[0], 0));
        System.out.println();
        for (int i = 1; i < 128; i += i) {

            System.out.println(getArraySize(1, i) + " bytes per p1[]");
            print("boolean[" + i +"]", new boolean[i]);

            System.out.println(getArraySize(2, i) + " bytes per p2[]");
            print("char[" + i +"]", new char[i]);
            print("short[" + i +"]", new short[i]);

            System.out.println(getArraySize(4, i) + " bytes per p4[]");
            print("int[" + i +"]", new int[i]);
            print("float[" + i +"]", new float[i]);

            System.out.println(getArraySize(8, i) + " bytes per p8[]");
            print("long[" + i +"]", new long[i]);
            print("double[" + i +"]", new double[i]);

            System.out.println(getArraySize(Constants.MEMORY_POINTER, i) +
                    " bytes per obj[]");
            print("Object[" + i +"]", new Object[i]);

            System.out.println();
        }
    }

    private static int getArraySize(int type, int length) {
        return ((Constants.MEMORY_OBJECT + length * type) + 7) / 8 * 8;
    }

    private static void print(String type, Object o) {
        System.out.println(getObjectSize(o) + " bytes per " + type);
    }

    /**
     * Get the number of bytes required for the given object.
     * This method only works if the agent is set.
     *
     * @param o the object
     * @return the number of bytes required
     */
    public static long getObjectSize(Object o) {
        Instrumentation inst = Profiler.getInstrumentation();
        return inst == null ? 0 : inst.getObjectSize(o);
    }

}
