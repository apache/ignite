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

package org.apache.ignite.spi.encryption.keystore;

import java.io.Serializable;
import java.security.Key;
import java.util.Arrays;
import java.util.Objects;
import org.jetbrains.annotations.Nullable;

/**
 * {@code EncryptionKey} implementation based on java security.
 *
 * @see Key
 * @see KeystoreEncryptionSpi
 */
public final class KeystoreEncryptionKey implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Encryption key.
     */
    private final Key k;

    /**
     * Key digest.
     */
    @Nullable final byte[] digest;

    /**
     * @param k Encryption key.
     * @param digest Message digest.
     */
    KeystoreEncryptionKey(Key k, @Nullable byte[] digest) {
        this.k = k;
        this.digest = digest;
    }

    /**
     * Encryption key.
     */
    public Key key() {
        return k;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        KeystoreEncryptionKey key = (KeystoreEncryptionKey)o;

        return Objects.equals(k, key.k) &&
            Arrays.equals(digest, key.digest);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(k);

        result = 31 * result + Arrays.hashCode(digest);

        return result;
    }
}
