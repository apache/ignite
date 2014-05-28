/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import org.gridgain.grid.util.typedef.*;

import java.io.*;

/**
 * Security credentials.
 */
public class GridSecurityCredentials implements Externalizable {
    /** Login. */
    private String login;

    /** Password. */
    private String password;

    /** Additional user object. */
    private Object userObj;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridSecurityCredentials() {
        // No-op.
    }

    /**
     * @param login Login.
     * @param password Password.
     */
    public GridSecurityCredentials(String login, String password) {
        this.login = login;
        this.password = password;
    }

    /**
     * @param login Login.
     * @param password Password.
     * @param userObj User object.
     */
    public GridSecurityCredentials(String login, String password, Object userObj) {
        this.login = login;
        this.password = password;
        this.userObj = userObj;
    }

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

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(login);
        out.writeUTF(password);
        out.writeObject(userObj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        login = in.readUTF();
        password = in.readUTF();
        userObj = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof GridSecurityCredentials))
            return false;

        GridSecurityCredentials that = (GridSecurityCredentials)o;

        return F.eq(login, that.login) && F.eq(password, that.password) && F.eq(userObj, that.userObj);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = login != null ? login.hashCode() : 0;

        res = 31 * res + (password != null ? password.hashCode() : 0);
        res = 31 * res + (userObj != null ? userObj.hashCode() : 0);

        return res;
    }
}
