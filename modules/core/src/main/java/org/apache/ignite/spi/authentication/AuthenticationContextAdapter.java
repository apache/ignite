/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.spi.authentication;

import org.apache.ignite.plugin.security.*;

import java.net.*;
import java.util.*;

/**
 * Authentication context.
 */
public class AuthenticationContextAdapter implements AuthenticationContext {
    /** Subject type. */
    private GridSecuritySubjectType subjType;

    /** Subject ID.w */
    private UUID subjId;

    /** Credentials. */
    private GridSecurityCredentials credentials;

    /** Subject address. */
    private InetSocketAddress addr;

    /**
     * Gets subject type.
     *
     * @return Subject type.
     */
    @Override public GridSecuritySubjectType subjectType() {
        return subjType;
    }

    /**
     * Sets subject type.
     *
     * @param subjType Subject type.
     */
    public void subjectType(GridSecuritySubjectType subjType) {
        this.subjType = subjType;
    }

    /**
     * Gets subject ID.
     *
     * @return Subject ID.
     */
    @Override public UUID subjectId() {
        return subjId;
    }

    /**
     * Sets subject ID.
     *
     * @param subjId Subject ID.
     */
    public void subjectId(UUID subjId) {
        this.subjId = subjId;
    }

    /**
     * Gets security credentials.
     *
     * @return Security credentials.
     */
    @Override public GridSecurityCredentials credentials() {
        return credentials;
    }

    /**
     * Sets security credentials.
     *
     * @param credentials Security credentials.
     */
    public void credentials(GridSecurityCredentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Gets subject network address.
     *
     * @return Subject network address.
     */
    @Override public InetSocketAddress address() {
        return addr;
    }

    /**
     * Sets subject network address.
     *
     * @param addr Subject network address.
     */
    public void address(InetSocketAddress addr) {
        this.addr = addr;
    }
}
