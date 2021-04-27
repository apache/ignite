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

package org.apache.ignite.internal.vault.common;

import java.io.Serializable;
import org.jetbrains.annotations.NotNull;

/**
 * Representation of vault entry.
 */
public class VaultEntry implements Serializable {
    /** Key. */
    private byte[] key;

    /** Value. */
    private byte[] val;

    /**
     * Constructs {@code VaultEntry} instance from the given key and value.
     *
     * @param key Key as a {@code ByteArray}.
     * @param val Value as a {@code byte[]}.
     */
    public VaultEntry(byte[] key, byte[] val) {
        this.key = key;
        this.val = val;
    }

    /**
     * Gets a key bytes.
     *
     * @return Byte array.
     */
    public @NotNull byte[] key() {
        return key;
    }

    /**
     * Gets a value bytes.
     *
     * @return Byte array.
     */
    public @NotNull byte[] value() {
        return val;
    }

    /**
     * Returns value which denotes whether entry is empty or not.
     *
     * @return {@code True} if entry is empty, otherwise - {@code false}.
     */
    public boolean empty() {
        return val == null;
    }
}
