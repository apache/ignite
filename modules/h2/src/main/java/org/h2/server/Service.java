/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.server;

import java.sql.SQLException;

/**
 * Classes implementing this interface usually provide a
 * TCP/IP listener such as an FTP server.
 * The can be started and stopped, and may or may not
 * allow remote connections.
 */
public interface Service {

    /**
     * Initialize the service from command line options.
     *
     * @param args the command line options
     */
    void init(String... args) throws Exception;

    /**
     * Get the URL of this service in a human readable form
     *
     * @return the url
     */
    String getURL();

    /**
     * Start the service. This usually means create the server socket.
     * This method must not block.
     */
    void start() throws SQLException;

    /**
     * Listen for incoming connections.
     * This method blocks.
     */
    void listen();

    /**
     * Stop the service.
     */
    void stop();

    /**
     * Check if the service is running.
     *
     * @param traceError if errors should be written
     * @return if the server is running
     */
    boolean isRunning(boolean traceError);

    /**
     * Check if remote connections are allowed.
     *
     * @return true if remote connections are allowed
     */
    boolean getAllowOthers();

    /**
     * Get the human readable name of the service.
     *
     * @return the name
     */
    String getName();

    /**
     * Get the human readable short name of the service.
     *
     * @return the type
     */
    String getType();

    /**
     * Gets the port this service is listening on.
     *
     * @return the port
     */
    int getPort();

    /**
     * Check if a daemon thread should be used.
     *
     * @return true if a daemon thread should be used
     */
    boolean isDaemon();

}
