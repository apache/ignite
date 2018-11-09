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

package org.apache.ignite.internal.processors.authentication;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;
import org.mindrot.BCrypt;

/**
 */
public class User implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Default user name. */
    public static final String DFAULT_USER_NAME = "ignite";

    /** Default user password. */
    private static final String DFLT_USER_PASSWORD = "ignite";

    /**
     * @see BCrypt#GENSALT_DEFAULT_LOG2_ROUNDS
     */
    private static int bCryptGensaltLog2Rounds = 10;

    /** User name. */
    private String name;

    /** Hashed password. */
    @GridToStringExclude
    private String hashedPasswd;

    /**
     * Constructor.
     */
    public User() {
    }

    /**
     * @param name User name.
     * @param hashedPasswd Hashed password.
     */
    private User(String name, String hashedPasswd) {
        this.name = name;
        this.hashedPasswd = hashedPasswd;
    }

    /**
     * @return User name.
     */
    public String name() {
        return name;
    }

    /**
     * Create new user.
     * @param name User name.
     * @param passwd Plain text password.
     * @return Created user.
     */
    public static User create(String name, String passwd) {
        return new User(name, password(passwd));
    }

    /**
     * Create empty user by login name.
     * @param name User name.
     * @return User.
     */
    public static User create(String name) {
        return new User(name, null);
    }

    /**
     * Create new user.
     *
     * @return Created user.
     */
    public static User defaultUser() {
        return create(DFAULT_USER_NAME, DFLT_USER_PASSWORD);
    }

    /**
     * @param passwd Plain text password.
     * @return Hashed password.
     */
    @Nullable public static String password(String passwd) {
        return password_bcrypt(passwd);
    }

    /**
     * @param passwd Plain text password.
     * @return Hashed password.
     */
    @Nullable private static String password_bcrypt(String passwd) {
        return BCrypt.hashpw(passwd, BCrypt.gensalt(bCryptGensaltLog2Rounds));
    }

    /**
     * @param passwd Plain text password.
     * @return {@code true} If user authorized, otherwise returns {@code false}.
     */
    public boolean authorize(String passwd) {
        if (F.isEmpty(passwd))
            return hashedPasswd == null;

        return BCrypt.checkpw(passwd, hashedPasswd);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        User u = (User)o;

        return F.eq(name, u.name) && F.eq(hashedPasswd, u.hashedPasswd);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(name, hashedPasswd);
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(User.class, this);
    }
}
