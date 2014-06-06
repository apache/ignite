/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.security;

import org.gridgain.grid.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Security credentials used for client or node authentication. Security credentials
 * are provided by {@link GridSecurityCredentialsProvider} which is specified on
 * client or node startup in configuration.
 * <p>
 * For grid node, security credentials provider is specified in
 * {@link GridConfiguration#setSecurityCredentialsProvider(GridSecurityCredentialsProvider)}
 * configuration property. For Java clients, you can provide credentials in
 * {@code GridClientConfiguration.setSecurityCredentialsProvider(...)} method.
 * <p>
 * Getting credentials through {@link GridSecurityCredentialsProvider} abstraction allows
 * users to provide custom implementations for storing user names and passwords in their
 * environment, possibly in encrypted format. GridGain comes with
 * {@link GridSecurityCredentialsBasicProvider} which simply provides
 * the passed in {@code login} and {@code password} when encryption or custom logic is not required.
 * <p>
 * In addition to {@code login} and {@code password}, security credentials allow for
 * specifying {@link #setUserObject(Object) userObject} as well, which can be used
 * to pass in any additional information required for authentication.
 */
// TODO: 8491 (need be portable to make client security tests pass).
public class GridSecurityCredentials implements Externalizable {
    /** Login. */
    private String login;

    /** Password. */
    @GridToStringExclude
    private String password;

    /** Additional user object. */
    @GridToStringExclude
    private Object userObj;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridSecurityCredentials() {
        // No-op.
    }

    /**
     * Constructs security credentials based on {@code login} and {@code password}.
     *
     * @param login Login.
     * @param password Password.
     */
    public GridSecurityCredentials(String login, String password) {
        this.login = login;
        this.password = password;
    }

    /**
     * Constructs security credentials based on {@code login}, {@code password},
     * and custom user object.
     *
     * @param login Login.
     * @param password Password.
     * @param userObj User object.
     */
    public GridSecurityCredentials(String login, String password, @Nullable Object userObj) {
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
    @Nullable public Object getUserObject() {
        return userObj;
    }

    /**
     * Sets user-specific object.
     *
     * @param userObj User object.
     */
    public void setUserObject(@Nullable Object userObj) {
        this.userObj = userObj;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, login);
        U.writeString(out, password);
        out.writeObject(userObj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        login = U.readString(in);
        password = U.readString(in);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridSecurityCredentials.class, this);
    }
}
