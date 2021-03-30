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

package org.apache.ignite.internal.metastorage.client;

import java.util.Arrays;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.metastorage.client.Entry;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Meta storage entry.
 */
public final class EntryImpl implements Entry {
    /** Key. */
    @NotNull
    private final ByteArray key;

    /** Value. */
    @Nullable
    private final byte[] val;

    /** Revision. */
    private final long rev;

    /** Update counter. */
    private final long updCntr;

    /**
     * Construct entry with given paramteters.
     *
     * @param key Key.
     * @param val Value.
     * @param rev Revision.
     * @param updCntr Update counter.
     */
    EntryImpl(@NotNull ByteArray key, @Nullable byte[] val, long rev, long updCntr) {
        this.key = key;
        this.val = val;
        this.rev = rev;
        this.updCntr = updCntr;
    }

    /** {@inheritDoc} */    @NotNull
    @Override public ByteArray key() {
        return key;
    }

    /** {@inheritDoc} */
    @Nullable
    @Override public byte[] value() {
        return val;
    }

    /** {@inheritDoc} */
    @Override public long revision() {
        return rev;
    }

    /** {@inheritDoc} */
    @Override public long updateCounter() {
        return updCntr;
    }

    /** {@inheritDoc} */
    @Override public boolean tombstone() {
        return val == null && rev > 0 && updCntr > 0;
    }

    /** {@inheritDoc} */
    @Override public boolean empty() {
        return val == null && rev == 0 && updCntr == 0;
    }


    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        EntryImpl entry = (EntryImpl)o;

        if (rev != entry.rev)
            return false;

        if (updCntr != entry.updCntr)
            return false;

        if (!key.equals(entry.key))
            return false;

        return Arrays.equals(val, entry.val);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = key.hashCode();

        res = 31 * res + Arrays.hashCode(val);

        res = 31 * res + (int)(rev ^ (rev >>> 32));

        res = 31 * res + (int)(updCntr ^ (updCntr >>> 32));

        return res;
    }
}
