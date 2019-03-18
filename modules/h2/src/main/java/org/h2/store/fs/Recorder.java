/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.store.fs;

/**
 * A recorder for the recording file system.
 */
public interface Recorder {

    /**
     * Create a new file.
     */
    int CREATE_NEW_FILE = 2;

    /**
     * Create a temporary file.
     */
    int CREATE_TEMP_FILE = 3;

    /**
     * Delete a file.
     */
    int DELETE = 4;

    /**
     * Open a file output stream.
     */
    int OPEN_OUTPUT_STREAM = 5;

    /**
     * Rename a file. The file name contains the source and the target file
     * separated with a colon.
     */
    int RENAME = 6;

    /**
     * Truncate the file.
     */
    int TRUNCATE = 7;

    /**
     * Write to the file.
     */
    int WRITE = 8;

    /**
     * Record the method.
     *
     * @param op the operation
     * @param fileName the file name or file name list
     * @param data the data or null
     * @param x the value or 0
     */
    void log(int op, String fileName, byte[] data, long x);

}
