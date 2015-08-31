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

package org.apache.ignite.plugin.security;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Security credentials used for node authentication. Security credentials
 * are provided by {@link SecurityCredentialsProvider} which is specified on
 * node startup in configuration.
 * <p>
 * Getting credentials through {@link SecurityCredentialsProvider} abstraction allows
 * users to provide custom implementations for storing user names and passwords in their
 * environment, possibly in encrypted format. Ignite comes with
 * {@link SecurityCredentialsBasicProvider} which simply provides
 * the passed in {@code login} and {@code password} when encryption or custom logic is not required.
 * <p>
 * In addition to {@code login} and {@code password}, security credentials allow for
 * specifying {@link #setUserObject(Object) userObject} as well, which can be used
 * to pass in any additional information required for authentication.
 */
public class SecurityCredentials implements Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Login. */
    private Object login;

    /** Password. */
    @GridToStringExclude
    private Object password;

    /** Additional user object. */
    @GridToStringExclude
    private Object userObj;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public SecurityCredentials() {
        // No-op.
    }

    /**
     * Constructs security credentials based on {@code login} and {@code password}.
     *
     * @param login Login.
     * @param password Password.
     */
    public SecurityCredentials(
        String login,
        String password
    ) {
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
    public SecurityCredentials(
        String login,
        String password,
        @Nullable Object userObj
    ) {
        this.login = login;
        this.password = password;
        this.userObj = userObj;
    }

    /**
     * Gets login.
     *
     * @return Login.
     */
    public Object getLogin() {
        return login;
    }

    /**
     * Sets login.
     *
     * @param login Login.
     */
    public void setLogin(Object login) {
        this.login = login;
    }

    /**
     * Gets password.
     *
     * @return Password.
     */
    public Object getPassword() {
        return password;
    }

    /**
     * Sets password.
     *
     * @param password Password.
     */
    public void setPassword(Object password) {
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
        out.writeObject(login);
        out.writeObject(password);
        out.writeObject(userObj);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        login = in.readObject();
        password = in.readObject();
        userObj = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (!(o instanceof SecurityCredentials))
            return false;

        SecurityCredentials that = (SecurityCredentials)o;

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
        return S.toString(SecurityCredentials.class, this);
    }
}