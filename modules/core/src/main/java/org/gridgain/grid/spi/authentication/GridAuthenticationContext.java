/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.authentication;

import org.gridgain.grid.security.*;
import org.gridgain.grid.spi.*;

import java.net.*;
import java.util.*;

/**
 * Authentication context.
 */
public class GridAuthenticationContext {
    /** Subject type. */
    private GridSecuritySubjectType subjType;

    /** Subject ID.w */
    private UUID subjId;

    /** Credentials. */
    private GridSecurityCredentials credentials;

    /** Subject address. */
    private InetAddress addr;

    /** Subject port. */
    private int port;

    /**
     * Gets subject type.
     *
     * @return Subject type.
     */
    public GridSecuritySubjectType subjectType() {
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
    public UUID subjectId() {
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
    public GridSecurityCredentials credentials() {
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
    public InetAddress address() {
        return addr;
    }

    /**
     * Sets subject network address.
     *
     * @param addr Subject network address.
     */
    public void address(InetAddress addr) {
        this.addr = addr;
    }

    /**
     * Gets subject port.
     *
     * @return Port.
     */
    public int port() {
        return port;
    }

    /**
     * Sets subject port.
     *
     * @param port Subject port.
     */
    public void port(int port) {
        this.port = port;
    }
}
