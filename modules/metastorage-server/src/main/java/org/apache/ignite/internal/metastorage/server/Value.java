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

package org.apache.ignite.internal.metastorage.server;

import org.jetbrains.annotations.NotNull;

/**
 * Meta storage value with update counter.
 */
public class Value {
    /** Tombstone constant. */
    public static final byte[] TOMBSTONE = new byte[0];

    /** Value. */
    private final byte[] bytes;

    /** Update counter. */
    private final long updCntr;

    /**
     * Constructs value.
     *
     * @param bytes   Value bytes or {@code #TOMBSTONE}.
     * @param updCntr Update cpunter.
     */
    public Value(@NotNull byte[] bytes, long updCntr) {
        this.bytes = bytes;
        this.updCntr = updCntr;
    }

    /**
     * Returns value bytes.
     *
     * @return Value bytes.
     */
    public byte[] bytes() {
        return bytes;
    }

    /**
     * Returns update counter.
     *
     * @return Update counter.
     */
    public long updateCounter() {
        return updCntr;
    }

    /**
     * Returns {@code true} if value is tombstone.
     *
     * @return {@code True} if value is tombstone, otherwise - {@code false}.
     */
    public boolean tombstone() {
        return bytes == TOMBSTONE;
    }
}
