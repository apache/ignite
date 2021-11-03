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

package org.apache.ignite.internal.metastorage.common.command;

import java.io.Serializable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines response for command which returns exactly one result (entry).
 */
public class SingleEntryResponse implements Serializable {
    /** Key. */
    @NotNull
    private final byte[] key;

    /** Value. */
    @Nullable
    private final byte[] val;

    /** Revision. */
    private final long rev;

    /** Update counter */
    private final long updCntr;

    /**
     * Constructs single entry response.
     *
     * @param key     Key. Couldn't be {@code null}.
     * @param val     Value. Could be {@code null} for empty and tombstone entries.
     * @param rev     Revision number.
     * @param updCntr Update counter.
     */
    public SingleEntryResponse(byte[] key, byte[] val, long rev, long updCntr) {
        this.key = key;
        this.val = val;
        this.rev = rev;
        this.updCntr = updCntr;
    }

    /**
     * Returns key.
     *
     * @return Entry key. Couldn't be {@code null}.
     */
    @NotNull
    public byte[] key() {
        return key;
    }

    /**
     * Returns value.
     *
     * @return Entry value. Could be {@code null} for empty and tombstone entries.
     */
    @Nullable
    public byte[] value() {
        return val;
    }

    /**
     * Returns revision.
     *
     * @return Entry revision.
     */
    public long revision() {
        return rev;
    }

    /**
     * Returns update counter.
     *
     * @return Entry update counter.
     */
    public long updateCounter() {
        return updCntr;
    }
}
