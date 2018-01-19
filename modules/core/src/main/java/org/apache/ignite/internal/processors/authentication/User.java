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

import com.sun.org.apache.xml.internal.security.utils.Base64;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Objects;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 */
public class User implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Salt length. */
    private static final int SALT_LEN = 16;

    /** Random. */
    private static final SecureRandom random = new SecureRandom();

    /** User name. */
    private final String name;

    /** Encrypted password. */
    @GridToStringExclude
    private String encPasswd;

    /** Salt. */
    @GridToStringExclude
    private byte[] salt;

    /** Accepted on cluster. */
    private boolean accepted;

    /**
     * @param name User name.
     * @param encPasswd Encoded password.
     * @param salt Salt.
     */
    private User(String name, String encPasswd, byte[] salt) {
        this.name = name;
        this.encPasswd = encPasswd;
        this.salt = salt;
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
        byte [] salt = new byte[SALT_LEN];

        random.nextBytes(salt);

        return new User(name, password(passwd, salt), salt);
    }

    /**
     * @param passwd Plain text password.
     * @param salt Salt.
     * @return Hashed password.
     */
    @Nullable public static String password(String passwd, byte[] salt) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-512");

            for (int i = 0; i < 64; ++i) {
                md.update(passwd.getBytes(StandardCharsets.UTF_8));

                md.update(salt);
            }

            return Base64.encode(md.digest(salt));
        } catch (NoSuchAlgorithmException e) {
            throw new IgniteException("Authentication error", e);
        }
    }

    /**
     * @param passwd Plain text password.
     * @return {@code true} If user authorized, otherwise returns {@code false}.
     */
    public boolean authorize(String passwd) {
        return encPasswd.equals(password(passwd, salt));
    }

    /**
     * @return Accepted on cluster flag.
     */
    public boolean accepted() {
        return accepted;
    }

    /**
     * @param accepted Accepted flag.
     */
    public void accepted(boolean accepted) {
        this.accepted = accepted;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        User u = (User)o;

        return F.eq(name, u.name) &&
            F.eq(salt, u.salt) &&
            F.eq(encPasswd, u.encPasswd);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(name, encPasswd, salt);
        return result;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(User.class, this);
    }
}
