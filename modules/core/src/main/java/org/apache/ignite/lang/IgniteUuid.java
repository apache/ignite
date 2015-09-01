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

package org.apache.ignite.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * This is a faster performing version of {@link UUID}. On basic tests this version is at least
 * 10x time faster for ID creation. It uses extra memory for 8-byte counter additionally to
 * internal UUID.
 */
public final class IgniteUuid implements Comparable<IgniteUuid>, Iterable<IgniteUuid>, Cloneable, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** VM ID. */
    public static final UUID VM_ID = UUID.randomUUID();

    /** */
    private static final AtomicLong cntGen = new AtomicLong(U.currentTimeMillis());

    /** */
    private UUID gid;

    /** */
    private long locId;

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public IgniteUuid() {
        // No-op.
    }

    /**
     * Constructs {@code GridUuid} from a global and local identifiers.
     *
     * @param gid UUID.
     * @param locId Counter.
     */
    public IgniteUuid(UUID gid, long locId) {
        assert gid != null;

        this.gid = gid;
        this.locId = locId;
    }

    /**
     * Gets {@link UUID} associated with local VM.
     *
     * @return {@link UUID} associated with local VM.
     */
    public static UUID vmId() {
        return VM_ID;
    }

    /**
     * Gets last generated local ID.
     *
     * @return Last generated local ID.
     */
    public static long lastLocalId() {
        return cntGen.get();
    }

    /**
     * Creates new pseudo-random ID.
     *
     * @return Newly created pseudo-random ID.
     */
    public static IgniteUuid randomUuid() {
        return new IgniteUuid(VM_ID, cntGen.incrementAndGet());
    }

    /**
     * Constructs new {@code GridUuid} based on global and local ID portions.
     *
     * @param id UUID instance.
     * @return Newly created pseudo-random ID.
     */
    public static IgniteUuid fromUuid(UUID id) {
        A.notNull(id, "id");

        return new IgniteUuid(id, cntGen.getAndIncrement());
    }

    /**
     * Converts string into {@code GridUuid}. The String must be in the format generated
     * by {@link #toString() GridUuid.toString()} method.
     *
     * @param s String to convert to {@code GridUuid}.
     * @return {@code GridUuid} instance representing given string.
     */
    public static IgniteUuid fromString(String s) {
        int firstDash = s.indexOf('-');

        return new IgniteUuid(
                UUID.fromString(s.substring(firstDash + 1)),
                Long.valueOf(new StringBuilder(s.substring(0, firstDash)).reverse().toString(), 16)
        );
    }


    /**
     * Gets a short string version of this ID. Use it only for UI where full version is
     * available to the application.
     *
     * @return Short string version of this ID.
     */
    public String shortString() {
        return new StringBuilder(Long.toHexString(locId)).reverse().toString();
    }

    /**
     * Gets global ID portion of this {@code GridUuid}.
     *
     * @return Global ID portion of this {@code GridUuid}.
     */
    public UUID globalId() {
        return gid;
    }

    /**
     * Gets local ID portion of this {@code GridUuid}.
     *
     * @return Local ID portion of this {@code GridUuid}.
     */
    public long localId() {
        return locId;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeUuid(out, gid);

        out.writeLong(locId);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException {
        gid = U.readUuid(in);

        locId = in.readLong();
    }

    /** {@inheritDoc} */
    @Override public int compareTo(IgniteUuid o) {
        if (o == this)
            return 0;

        if (o == null)
            return 1;

        int res = Long.compare(locId, o.locId);

        if (res == 0)
            res = gid.compareTo(o.globalId());

        return res;
    }

    /** {@inheritDoc} */
    @Override public GridIterator<IgniteUuid> iterator() {
        return F.iterator(Collections.singleton(this), F.<IgniteUuid>identity(), true);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (obj == this)
            return true;

        if (!(obj instanceof IgniteUuid))
            return false;

        IgniteUuid that = (IgniteUuid)obj;

        return that.locId == locId && that.gid.equals(gid);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return 31 * gid.hashCode() + (int)(locId ^ (locId >>> 32));
    }

    /** {@inheritDoc} */
    @Override public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return shortString() + '-' + gid.toString();
    }
}