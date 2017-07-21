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

package org.apache.ignite.internal.processors.cache.distributed;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.TimeUnit;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * Externalizable wrapper for {@link ExpiryPolicy}.
 */
public class IgniteExternalizableExpiryPolicy implements ExpiryPolicy, Externalizable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private ExpiryPolicy plc;

    /** */
    private static final byte CREATE_TTL_MASK = 0x01;

    /** */
    private static final byte UPDATE_TTL_MASK = 0x02;

    /** */
    private static final byte ACCESS_TTL_MASK = 0x04;

    /** */
    private Duration forCreate;

    /** */
    private Duration forUpdate;

    /** */
    private Duration forAccess;

    /**
     * Required by {@link Externalizable}.
     */
    public IgniteExternalizableExpiryPolicy() {
        // No-op.
    }

    /**
     * @param plc Expiry policy.
     */
    public IgniteExternalizableExpiryPolicy(ExpiryPolicy plc) {
        assert plc != null;

        this.plc = plc;
    }

    /** {@inheritDoc} */
    @Override public Duration getExpiryForCreation() {
        return forCreate;
    }

    /** {@inheritDoc} */
    @Override public Duration getExpiryForAccess() {
        return forAccess;
    }

    /** {@inheritDoc} */
    @Override public Duration getExpiryForUpdate() {
        return forUpdate;
    }

    /**
     * @param out Output stream.
     * @param duration Duration.
     * @throws IOException If failed.
     */
    private void writeDuration(ObjectOutput out, @Nullable Duration duration) throws IOException {
        if (duration != null) {
            if (duration.getDurationAmount() == 0L) {
                if (duration.isEternal())
                    out.writeLong(0);
                else
                    out.writeLong(CU.TTL_ZERO);
            }
            else
                out.writeLong(duration.getTimeUnit().toMillis(duration.getDurationAmount()));
        }
    }

    /**
     * @param in Input stream.
     * @return Duration.
     * @throws IOException If failed.
     */
    private Duration readDuration(ObjectInput in) throws IOException {
        long ttl = in.readLong();

        assert ttl >= 0 || ttl == CU.TTL_ZERO : ttl;

        if (ttl == 0)
            return Duration.ETERNAL;
        else if (ttl == CU.TTL_ZERO)
            return Duration.ZERO;

        return new Duration(TimeUnit.MILLISECONDS, ttl);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        byte flags = 0;

        Duration create = plc.getExpiryForCreation();

        if (create != null)
            flags |= CREATE_TTL_MASK;

        Duration update = plc.getExpiryForUpdate();

        if (update != null)
            flags |= UPDATE_TTL_MASK;

        Duration access = plc.getExpiryForAccess();

        if (access != null)
            flags |= ACCESS_TTL_MASK;

        out.writeByte(flags);

        writeDuration(out, create);

        writeDuration(out, update);

        writeDuration(out, access);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte flags = in.readByte();

        if ((flags & CREATE_TTL_MASK) != 0)
            forCreate = readDuration(in);

        if ((flags & UPDATE_TTL_MASK) != 0)
            forUpdate = readDuration(in);

        if ((flags & ACCESS_TTL_MASK) != 0)
            forAccess = readDuration(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(IgniteExternalizableExpiryPolicy.class, this);
    }
}