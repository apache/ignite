/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.store;

import org.h2.test.TestBase;

/**
 * Test using volatile fields to ensure we don't read from a version that is
 * concurrently written to.
 */
public class TestSpinLock extends TestBase {

    /**
     * The version to use for writing.
     */
    volatile int writeVersion;

    /**
     * The current data object.
     */
    volatile Data data = new Data(0, null);

    /**
     * Run just this test.
     *
     * @param a ignored
     */
    public static void main(String... a) throws Exception {
        TestBase.createCaller().init().test();
    }

    @Override
    public void test() throws Exception {
        final TestSpinLock obj = new TestSpinLock();
        Thread t = new Thread() {
            @Override
            public void run() {
                while (!isInterrupted()) {
                    for (int i = 0; i < 10000; i++) {
                        Data d = obj.copyOnWrite();
                        obj.data = d;
                        d.write(i);
                        d.writing = false;
                    }
                }
            }
        };
        t.start();
        try {
            for (int i = 0; i < 100000; i++) {
                Data d = obj.getImmutable();
                int z = d.x + d.y;
                if (z != 0) {
                    String error = i + " result: " + z + " now: " + d.x + " "
                            + d.y;
                    System.out.println(error);
                    throw new Exception(error);
                }
            }
        } finally {
            t.interrupt();
            t.join();
        }
    }

    /**
     * Clone the data object if necessary (if the write version is newer than
     * the current version).
     *
     * @return the data object
     */
    Data copyOnWrite() {
        Data d = data;
        d.writing = true;
        int w = writeVersion;
        if (w <= data.version) {
            return d;
        }
        Data d2 = new Data(w, data);
        d2.writing = true;
        d.writing = false;
        return d2;
    }

    /**
     * Get an immutable copy of the data object.
     *
     * @return the immutable object
     */
    private Data getImmutable() {
        Data d = data;
        ++writeVersion;
        // wait until writing is done,
        // but only for the current write operation:
        // a bit like a spin lock
        while (d.writing) {
            // Thread.yield() is not required, specially
            // if there are multiple cores
            // but getImmutable() doesn't
            // need to be that fast actually
            Thread.yield();
        }
        return d;
    }

    /**
     * The data class - represents the root page.
     */
    static class Data {

        /**
         * The version.
         */
        final int version;

        /**
         * The values.
         */
        int x, y;

        /**
         * Whether a write operation is in progress.
         */
        volatile boolean writing;

        /**
         * Create a copy of the data.
         *
         * @param version the new version
         * @param old the old data or null
         */
        Data(int version, Data old) {
            this.version = version;
            if (old != null) {
                this.x = old.x;
                this.y = old.y;
            }
        }

        /**
         * Write to the fields in an unsynchronized way.
         *
         * @param value the new value
         */
        void write(int value) {
            this.x = value;
            this.y = -value;
        }

    }

}
