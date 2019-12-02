/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.ftp.server;

/**
 * Event listener for the FTP Server.
 */
public interface FtpEventListener {

    /**
     * Called before the given command is processed.
     *
     * @param event the event
     */
    void beforeCommand(FtpEvent event);

    /**
     * Called after the command has been processed.
     *
     * @param event the event
     */
    void afterCommand(FtpEvent event);

    /**
     * Called when an unsupported command is processed.
     * This method is called after beforeCommand.
     *
     * @param event the event
     */
    void onUnsupportedCommand(FtpEvent event);
}
