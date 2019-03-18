/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.test.bench;

import java.sql.SQLException;

/**
 * The interface for benchmark tests.
 */
public interface Bench {

    /**
     * Initialize the database. This includes creating tables and inserting
     * data.
     *
     * @param db the database object
     * @param size the amount of data
     */
    void init(Database db, int size) throws SQLException;

    /**
     * Run the test.
     */
    void runTest() throws Exception;

    /**
     * Get the name of the test.
     *
     * @return the test name
     */
    String getName();

}
