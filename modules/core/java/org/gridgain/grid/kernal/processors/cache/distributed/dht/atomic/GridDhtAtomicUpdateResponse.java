/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.io.*;
import java.nio.*;
import java.util.*;

/**
 * DHT atomic cache backup update response.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDhtAtomicUpdateResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Future version. */
    private GridCacheVersion futVer;

    /** Failed keys. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> failedKeys;

    /** Serialized failed keys. */
    private byte[] failedKeysBytes;

    /** Update error. */
    @GridDirectTransient
    private GridMultiException err;

    /** Serialized update error. */
    private byte[] errBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridDhtAtomicUpdateResponse() {
        // No-op.
    }

    /**
     * @param futVer Future version.
     */
    public GridDhtAtomicUpdateResponse(GridCacheVersion futVer) {
        this.futVer = futVer;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Gets update error.
     */
    public GridMultiException error() {
        return err;
    }

    /**
     * @return Failed keys.
     */
    public Collection<K> failedKeys() {
        return failedKeys;
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    public void addFailedKey(K key, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ArrayList<>();

        failedKeys.add(key);

        if (err == null)
            err = new GridMultiException("Failed to update keys on primary node.");

        err.add(e);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        failedKeysBytes = ctx.marshaller().marshal(failedKeys);
        errBytes = ctx.marshaller().marshal(err);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        failedKeys = ctx.marshaller().unmarshal(failedKeysBytes, ldr);
        err = ctx.marshaller().unmarshal(errBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridDhtAtomicUpdateResponse _clone = new GridDhtAtomicUpdateResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridDhtAtomicUpdateResponse _clone = (GridDhtAtomicUpdateResponse)_msg;

        _clone.futVer = futVer;
        _clone.failedKeys = failedKeys;
        _clone.failedKeysBytes = failedKeysBytes;
        _clone.err = err;
        _clone.errBytes = errBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean writeTo(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.writeTo(buf))
            return false;

        if (!commState.typeWritten) {
            if (!commState.putByte(directType()))
                return false;

            commState.typeWritten = true;
        }

        switch (commState.idx) {
            case 2:
                if (!commState.putByteArray(errBytes))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putByteArray(failedKeysBytes))
                    return false;

                commState.idx++;

            case 4:
                if (!commState.putCacheVersion(futVer))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("all")
    @Override public boolean readFrom(ByteBuffer buf) {
        commState.setBuffer(buf);

        if (!super.readFrom(buf))
            return false;

        switch (commState.idx) {
            case 2:
                byte[] errBytes0 = commState.getByteArray();

                if (errBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                errBytes = errBytes0;

                commState.idx++;

            case 3:
                byte[] failedKeysBytes0 = commState.getByteArray();

                if (failedKeysBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                failedKeysBytes = failedKeysBytes0;

                commState.idx++;

            case 4:
                GridCacheVersion futVer0 = commState.getCacheVersion();

                if (futVer0 == CACHE_VER_NOT_READ)
                    return false;

                futVer = futVer0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 38;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDhtAtomicUpdateResponse.class, this);
    }
}
