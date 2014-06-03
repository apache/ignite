/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.cmd.dto.node;

import java.io.*;

/**
 * Data transfer object for node email configuration properties.
 */
public class VisorEmailConfig implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** SMTP host. */
    private final String smtpHost;

    /** SMTP port. */
    private final int smtpPort;

    /** SMTP user name. */
    private final String smtpUsername;

    /** SMTP admin emails. */
    private final String adminEmails;

    /** From email address. */
    private final String smtpFromEmail;

    /** Whether or not to use SSL for SMTP. */
    private final boolean smtpSsl;

    /** Whether or not to use TLS for SMTP. */
    private final boolean smtpStartTls;

    /** Create data transfer object with given parameters. */
    public VisorEmailConfig(String smtpHost, int smtpPort, String smtpUsername, String adminEmails,
        String smtpFromEmail, boolean smtpSsl, boolean smtpStartTls) {
        this.smtpHost = smtpHost;
        this.smtpPort = smtpPort;
        this.smtpUsername = smtpUsername;
        this.adminEmails = adminEmails;
        this.smtpFromEmail = smtpFromEmail;
        this.smtpSsl = smtpSsl;
        this.smtpStartTls = smtpStartTls;
    }

    /**
     * @return Smtp host.
     */
    public String smtpHost() {
        return smtpHost;
    }

    /**
     * @return Smtp port.
     */
    public int smtpPort() {
        return smtpPort;
    }

    /**
     * @return Smtp user name.
     */
    public String smtpUsername() {
        return smtpUsername;
    }

    /**
     * @return SMTP admin emails.
     */
    public String adminEmails() {
        return adminEmails;
    }

    /**
     * @return From email address.
     */
    public String smtpFromEmail() {
        return smtpFromEmail;
    }

    /**
     * @return Whether or not to use SSL for SMTP.
     */
    public boolean smtpSsl() {
        return smtpSsl;
    }

    /**
     * @return Whether or not to use TLS for SMTP.
     */
    public boolean smtpStartTls() {
        return smtpStartTls;
    }
}
