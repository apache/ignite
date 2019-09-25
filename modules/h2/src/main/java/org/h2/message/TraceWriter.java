/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.message;

/**
 * The backend of the trace system must implement this interface. Two
 * implementations are supported: the (default) native trace writer
 * implementation that can write to a file and to system out, and an adapter
 * that uses SLF4J (Simple Logging Facade for Java).
 */
interface TraceWriter {

    /**
     * Set the name of the database or trace object.
     *
     * @param name the new name
     */
    void setName(String name);

    /**
     * Write a message.
     *
     * @param level the trace level
     * @param module the name of the module
     * @param s the message
     * @param t the exception (may be null)
     */
    void write(int level, String module, String s, Throwable t);

    /**
     * Write a message.
     *
     * @param level the trace level
     * @param moduleId the id of the module
     * @param s the message
     * @param t the exception (may be null)
     */
    void write(int level, int moduleId, String s, Throwable t);


    /**
     * Check the given trace / log level is enabled.
     *
     * @param level the level
     * @return true if the level is enabled
     */
    boolean isEnabled(int level);

}
