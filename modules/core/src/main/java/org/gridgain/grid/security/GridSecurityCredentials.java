/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import java.net.*;

/**
 * Security credentials.
 */
public class GridSecurityCredentials {
    /** Login. */
    private String login;

    /** Password. */
    private String password;

    /** Inet address. Automatically set by GridGain if available. */
    private InetAddress addr;

    /** Additional user object. */
    private Object userObj;

    /**
     * Gets login.
     *
     * @return Login.
     */
    public String getLogin() {
        return login;
    }

    /**
     * Sets login.
     *
     * @param login Login.
     */
    public void setLogin(String login) {
        this.login = login;
    }

    /**
     * Gets password.
     *
     * @return Password.
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets password.
     *
     * @param password Password.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Gets network address of subject, if available. Network address is provided by GridGain.
     *
     * @return Subject network address.
     */
    public InetAddress getAddress() {
        return addr;
    }

    /**
     * Sets subject network address. Should not be used bu user.
     *
     * @param addr Address.
     */
    public void setAddress(InetAddress addr) {
        this.addr = addr;
    }

    /**
     * Gets user-specific object.
     *
     * @return User object.
     */
    public Object getUserObject() {
        return userObj;
    }

    /**
     * Sets user-specific object.
     *
     * @param userObj User object.
     */
    public void setUserObject(Object userObj) {
        this.userObj = userObj;
    }
}
