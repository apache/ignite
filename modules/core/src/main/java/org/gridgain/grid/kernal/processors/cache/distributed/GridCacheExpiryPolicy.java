/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import javax.cache.expiry.*;
import java.io.*;
import java.util.concurrent.*;

/**
 *
 */
public class GridCacheExpiryPolicy implements ExpiryPolicy, Externalizable {
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
    public GridCacheExpiryPolicy() {
        // No-op.
    }

    /**
     * @param plc Expiry policy.
     */
    public GridCacheExpiryPolicy(ExpiryPolicy plc) {
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
     * @throws IOException
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
     * @throws IOException
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
        return S.toString(GridCacheExpiryPolicy.class, this);
    }
}
