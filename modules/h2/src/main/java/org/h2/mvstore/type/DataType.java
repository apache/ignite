/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.mvstore.type;

import java.nio.ByteBuffer;

import org.h2.mvstore.WriteBuffer;

/**
 * A data type.
 */
public interface DataType {

    /**
     * Compare two keys.
     *
     * @param a the first key
     * @param b the second key
     * @return -1 if the first key is smaller, 1 if larger, and 0 if equal
     * @throws UnsupportedOperationException if the type is not orderable
     */
    int compare(Object a, Object b);

    /**
     * Estimate the used memory in bytes.
     *
     * @param obj the object
     * @return the used memory
     */
    int getMemory(Object obj);

    /**
     * Write an object.
     *
     * @param buff the target buffer
     * @param obj the value
     */
    void write(WriteBuffer buff, Object obj);

    /**
     * Write a list of objects.
     *
     * @param buff the target buffer
     * @param obj the objects
     * @param len the number of objects to write
     * @param key whether the objects are keys
     */
    void write(WriteBuffer buff, Object[] obj, int len, boolean key);

    /**
     * Read an object.
     *
     * @param buff the source buffer
     * @return the object
     */
    Object read(ByteBuffer buff);

    /**
     * Read a list of objects.
     *
     * @param buff the target buffer
     * @param obj the objects
     * @param len the number of objects to read
     * @param key whether the objects are keys
     */
    void read(ByteBuffer buff, Object[] obj, int len, boolean key);

}

