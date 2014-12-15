/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

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
    private Duration forCreate;

    /** */
    private Duration forUpdate;

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
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public Duration getExpiryForUpdate() {
        return forUpdate;
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

        out.writeByte(flags);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        byte flags = in.readByte();

        if ((flags & CREATE_TTL_MASK) != 0)
            forCreate = new Duration(TimeUnit.MILLISECONDS, in.readLong());

        if ((flags & UPDATE_TTL_MASK) != 0)
            forUpdate = new Duration(TimeUnit.MILLISECONDS, in.readLong());
    }
}
