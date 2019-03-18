/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.dev.ftp.server;

/**
 * Describes an FTP event. This class is used by the FtpEventListener.
 */
public class FtpEvent {
    private final FtpControl control;
    private final String command;
    private final String param;

    FtpEvent(FtpControl control, String command, String param) {
        this.control = control;
        this.command = command;
        this.param = param;
    }

    /**
     * Get the FTP command. Example: RETR
     *
     * @return the command
     */
    public String getCommand() {
        return command;
    }

    /**
     * Get the FTP control object.
     *
     * @return the control object
     */
    public FtpControl getControl() {
        return control;
    }

    /**
     * Get the parameter of the FTP command (if any).
     *
     * @return the parameter
     */
    public String getParam() {
        return param;
    }
}
