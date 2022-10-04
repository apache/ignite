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

package org.apache.ignite.internal.managers.encryption;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * Cache group encryption key with identifier. Key is encrypted.
 */
public class GroupKeyEncrypted implements Serializable {
    /** Serial version UID. */
    private static final long serialVersionUID = 0L;

    /** Encryption key ID. */
    private final int id;

    /** Encryption key. */
    private final byte[] key;

    /**
     * @param id Encryption key ID.
     * @param key Encryption key.
     */
    public GroupKeyEncrypted(int id, byte[] key) {
        this.id = id;
        this.key = key;
    }

    /**
     * @return Encryption key ID.
     */
    public int id() {
        return id;
    }

    /**
     * @return Encryption key.
     */
    public byte[] key() {
        return key;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        GroupKeyEncrypted encrypted = (GroupKeyEncrypted)o;

        return id == encrypted.id && Arrays.equals(key, encrypted.key);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(id);

        result = 31 * result + Arrays.hashCode(key);

        return result;
    }
}
