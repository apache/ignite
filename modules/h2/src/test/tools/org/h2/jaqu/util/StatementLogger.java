/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: James Moger
 */
package org.h2.jaqu.util;

import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Utility class to optionally log generated statements to an output stream.<br>
 * Default output stream is System.out.<br>
 * Statement logging is disabled by default.
 * <p>
 * This class also tracks the counts for generated statements by major type.
 *
 */
public class StatementLogger {

    public static boolean logStatements;
    private static final PrintWriter OUT = new PrintWriter(System.out);
    private static final AtomicLong SELECT_COUNT = new AtomicLong();
    private static final AtomicLong CREATE_COUNT = new AtomicLong();
    private static final AtomicLong INSERT_COUNT = new AtomicLong();
    private static final AtomicLong UPDATE_COUNT = new AtomicLong();
    private static final AtomicLong MERGE_COUNT = new AtomicLong();
    private static final AtomicLong DELETE_COUNT = new AtomicLong();

    public static void create(String statement) {
        CREATE_COUNT.incrementAndGet();
        log(statement);
    }

    public static void insert(String statement) {
        INSERT_COUNT.incrementAndGet();
        log(statement);
    }

    public static void update(String statement) {
        UPDATE_COUNT.incrementAndGet();
        log(statement);
    }

    public static void merge(String statement) {
        MERGE_COUNT.incrementAndGet();
        log(statement);
    }

    public static void delete(String statement) {
        DELETE_COUNT.incrementAndGet();
        log(statement);
    }

    public static void select(String statement) {
        SELECT_COUNT.incrementAndGet();
        log(statement);
    }

    private static void log(String statement) {
        if (logStatements) {
            OUT.println(statement);
        }
    }

    public static void printStats() {
        OUT.println("JaQu Runtime Statistics");
        OUT.println("=======================");
        printStat("CREATE", CREATE_COUNT);
        printStat("INSERT", INSERT_COUNT);
        printStat("UPDATE", UPDATE_COUNT);
        printStat("MERGE", MERGE_COUNT);
        printStat("DELETE", DELETE_COUNT);
        printStat("SELECT", SELECT_COUNT);
    }

    private static void printStat(String name, AtomicLong value) {
        if (value.get() > 0) {
            DecimalFormat df = new DecimalFormat("###,###,###,###");
            OUT.println(name + "=" + df.format(CREATE_COUNT.get()));
        }
    }

}