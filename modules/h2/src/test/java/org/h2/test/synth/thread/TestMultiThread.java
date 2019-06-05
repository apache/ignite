/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.synth.thread;

import java.sql.SQLException;
import java.util.Random;

import org.h2.test.TestBase;

/**
 * The is an abstract operation for {@link TestMulti}.
 */
abstract class TestMultiThread extends Thread {

    /**
     * The base object.
     */
    TestMulti base;

    /**
     * The random number generator.
     */
    Random random = new Random();

    TestMultiThread(TestMulti base) {
        this.base = base;
    }

    /**
     * Execute statements that need to be executed before starting the thread.
     * This includes CREATE TABLE statements.
     */
    abstract void first() throws SQLException;

    /**
     * The main operation to perform. This method is called in a loop.
     */
    abstract void operation() throws SQLException;

    /**
     * Execute statements before entering the loop, but after starting the
     * thread.
     */
    abstract void begin() throws SQLException;

    /**
     * This method is called once after the test is stopped.
     */
    abstract void end() throws SQLException;

    /**
     * This method is called once after all threads have been stopped.
     */
    abstract void finalTest() throws SQLException;

    @Override
    public void run() {
        try {
            begin();
            while (!base.stop) {
                operation();
            }
            end();
        } catch (Throwable e) {
            TestBase.logError("error", e);
        }
    }

}
