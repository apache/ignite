/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.visor.node;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;

import static java.lang.System.*;
import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.*;

/**
 * Data transfer object for node email configuration properties.
 */
public class VisorEmailConfiguration implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** SMTP host. */
    private String smtpHost;

    /** SMTP port. */
    private int smtpPort;

    /** SMTP user name. */
    private String smtpUsername;

    /** SMTP admin emails. */
    private String adminEmails;

    /** From email address. */
    private String smtpFromEmail;

    /** Whether or not to use SSL for SMTP. */
    private boolean smtpSsl;

    /** Whether or not to use TLS for SMTP. */
    private boolean smtpStartTls;

    /**
     * @param c Grid configuration.
     * @return Data transfer object for node email configuration properties.
     */
    public static VisorEmailConfiguration from(IgniteConfiguration c) {
        VisorEmailConfiguration cfg = new VisorEmailConfiguration();

        cfg.smtpHost(getProperty(GG_SMTP_HOST, c.getSmtpHost()));
        cfg.smtpPort(intValue(GG_SMTP_PORT, c.getSmtpPort()));
        cfg.smtpUsername(getProperty(GG_SMTP_USERNAME, c.getSmtpUsername()));
        cfg.adminEmails(getProperty(GG_ADMIN_EMAILS, compactArray(c.getAdminEmails())));
        cfg.smtpFromEmail(getProperty(GG_SMTP_FROM, c.getSmtpFromEmail()));
        cfg.smtpSsl(boolValue(GG_SMTP_SSL, c.isSmtpSsl()));
        cfg.smtpStartTls(boolValue(GG_SMTP_STARTTLS, c.isSmtpStartTls()));

        return cfg;
    }

    /**
     * @return SMTP host.
     */
    public String smtpHost() {
        return smtpHost;
    }

    /**
     * @param smtpHost New SMTP host.
     */
    public void smtpHost(String smtpHost) {
        this.smtpHost = smtpHost;
    }

    /**
     * @return SMTP port.
     */
    public int smtpPort() {
        return smtpPort;
    }

    /**
     * @param smtpPort New SMTP port.
     */
    public void smtpPort(int smtpPort) {
        this.smtpPort = smtpPort;
    }

    /**
     * @return SMTP user name.
     */
    public String smtpUsername() {
        return smtpUsername;
    }

    /**
     * @param smtpUsername New SMTP user name.
     */
    public void smtpUsername(String smtpUsername) {
        this.smtpUsername = smtpUsername;
    }

    /**
     * @return SMTP admin emails.
     */
    public String adminEmails() {
        return adminEmails;
    }

    /**
     * @param adminEmails New SMTP admin emails.
     */
    public void adminEmails(String adminEmails) {
        this.adminEmails = adminEmails;
    }

    /**
     * @return From email address.
     */
    public String smtpFromEmail() {
        return smtpFromEmail;
    }

    /**
     * @param smtpFromEmail New from email address.
     */
    public void smtpFromEmail(String smtpFromEmail) {
        this.smtpFromEmail = smtpFromEmail;
    }

    /**
     * @return Whether or not to use SSL for SMTP.
     */
    public boolean smtpSsl() {
        return smtpSsl;
    }

    /**
     * @param smtpSsl New whether or not to use SSL for SMTP.
     */
    public void smtpSsl(boolean smtpSsl) {
        this.smtpSsl = smtpSsl;
    }

    /**
     * @return Whether or not to use TLS for SMTP.
     */
    public boolean smtpStartTls() {
        return smtpStartTls;
    }

    /**
     * @param smtpStartTls New whether or not to use TLS for SMTP.
     */
    public void smtpStartTls(boolean smtpStartTls) {
        this.smtpStartTls = smtpStartTls;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorEmailConfiguration.class, this);
    }
}
