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

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.io.*;
import java.util.concurrent.*;

/**
 * Externalizable wrapper for {@link ExpiryPolicy}.
 */
public class IgniteExternalizableExpiryPolicy implements ExpiryPolicy, Externalizable, IgniteOptimizedMarshallable {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @SuppressWarnings({"NonConstantFieldWithUpperCaseName", "AbbreviationUsage", "UnusedDeclaration"})
    private static Object GG_CLASS_ID;

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
    @Override public Object ggClassId() {
        return GG_CLASS_ID;
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
            if (duration.isEternal())
                out.writeLong(0);
            else if (duration.getDurationAmount() == 0)
                out.writeLong(1);
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

        assert ttl >= 0;

        if (ttl == 0)
            return Duration.ETERNAL;

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
