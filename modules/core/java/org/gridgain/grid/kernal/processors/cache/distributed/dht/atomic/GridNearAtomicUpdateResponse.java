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
import java.util.concurrent.*;

/**
 * DHT atomic cache near update response.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridNearAtomicUpdateResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Cache message index. */
    public static final int CACHE_MSG_IDX = nextIndexId();

    /** Node ID this reply should be sent to. */
    @GridDirectTransient
    private UUID nodeId;

    /** Future version. */
    private GridCacheVersion futVer;

    /** Update error. */
    @GridDirectTransient
    private volatile GridMultiException err;

    /** Serialized error. */
    private byte[] errBytes;

    /** Return value. */
    @GridDirectTransient
    private GridCacheReturn<V> retVal;

    /** Serialized return value. */
    private byte[] retValBytes;

    /** Failed keys. */
    @GridToStringInclude
    @GridDirectTransient
    private volatile Collection<K> failedKeys;

    /** Serialized failed keys. */
    private byte[] failedKeysBytes;

    /** Keys that should be remapped. */
    @GridToStringInclude
    @GridDirectTransient
    private Collection<K> remapKeys;

    /** Serialized keys that should be remapped. */
    private byte[] remapKeysBytes;

    /**
     * Empty constructor required by {@link Externalizable}.
     */
    public GridNearAtomicUpdateResponse() {
        // No-op.
    }

    /**
     * @param nodeId Node ID this reply should be sent to.
     * @param futVer Future version.
     */
    public GridNearAtomicUpdateResponse(UUID nodeId, GridCacheVersion futVer) {
        this.nodeId = nodeId;
        this.futVer = futVer;
    }

    /** {@inheritDoc} */
    @Override public int lookupIndex() {
        return CACHE_MSG_IDX;
    }

    /**
     * @return Node ID this response should be sent to.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @param nodeId Node ID.
     */
    public void nodeId(UUID nodeId) {
        this.nodeId = nodeId;
    }

    /**
     * @return Future version.
     */
    public GridCacheVersion futureVersion() {
        return futVer;
    }

    /**
     * @return Update error, if any.
     */
    public Throwable error() {
        return err;
    }

    /**
     * @return Collection of failed keys.
     */
    public Collection<K> failedKeys() {
        return failedKeys;
    }

    /**
     * @return Return value.
     */
    public GridCacheReturn<V> returnValue() {
        return retVal;
    }

    /**
     * @param retVal Return value.
     */
    public void returnValue(GridCacheReturn<V> retVal) {
        this.retVal = retVal;
    }

    /**
     * @param remapKeys Remap keys.
     */
    public void remapKeys(Collection<K> remapKeys) {
        this.remapKeys = remapKeys;
    }

    /**
     * @return Remap keys.
     */
    public Collection<K> remapKeys() {
        return remapKeys;
    }

    /**
     * Adds key to collection of failed keys.
     *
     * @param key Key to add.
     * @param e Error cause.
     */
    public synchronized void addFailedKey(K key, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ConcurrentLinkedQueue<>();

        failedKeys.add(key);

        if (err == null)
            err = new GridMultiException("Failed to update keys on primary node.");

        if (e instanceof GridMultiException) {
            for (Throwable th : ((GridMultiException)e).nestedCauses())
                err.add(th);
        }
        else
            err.add(e);
    }

    /**
     * Adds keys to collection of failed keys.
     *
     * @param keys Key to add.
     * @param e Error cause.
     */
    public synchronized void addFailedKeys(Collection<K> keys, Throwable e) {
        if (failedKeys == null)
            failedKeys = new ArrayList<>(keys.size());

        failedKeys.addAll(keys);

        if (err == null)
            err = new GridMultiException("Failed to update keys on primary node.");

        if (e instanceof GridMultiException) {
            for (Throwable th : ((GridMultiException)e).nestedCauses())
                err.add(th);
        }
        else
            err.add(e);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (err != null)
            errBytes = ctx.marshaller().marshal(err);

        if (retVal != null)
            retValBytes = ctx.marshaller().marshal(retVal);

        if (failedKeys != null)
            failedKeysBytes = ctx.marshaller().marshal(failedKeys);

        if (remapKeys != null)
            remapKeysBytes = ctx.marshaller().marshal(remapKeys);
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (errBytes != null)
            err = ctx.marshaller().unmarshal(errBytes, ldr);

        if (retValBytes != null)
            retVal = ctx.marshaller().unmarshal(retValBytes, ldr);

        if (failedKeysBytes != null)
            failedKeys = ctx.marshaller().unmarshal(failedKeysBytes, ldr);

        if (remapKeysBytes != null)
            remapKeys = ctx.marshaller().unmarshal(remapKeysBytes, ldr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridNearAtomicUpdateResponse _clone = new GridNearAtomicUpdateResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridNearAtomicUpdateResponse _clone = (GridNearAtomicUpdateResponse)_msg;

        _clone.nodeId = nodeId;
        _clone.futVer = futVer;
        _clone.err = err;
        _clone.errBytes = errBytes;
        _clone.retVal = retVal;
        _clone.retValBytes = retValBytes;
        _clone.failedKeys = failedKeys;
        _clone.failedKeysBytes = failedKeysBytes;
        _clone.remapKeys = remapKeys;
        _clone.remapKeysBytes = remapKeysBytes;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
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

            case 5:
                if (!commState.putByteArray(remapKeysBytes))
                    return false;

                commState.idx++;

            case 6:
                if (!commState.putByteArray(retValBytes))
                    return false;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("fallthrough")
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

            case 5:
                byte[] remapKeysBytes0 = commState.getByteArray();

                if (remapKeysBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                remapKeysBytes = remapKeysBytes0;

                commState.idx++;

            case 6:
                byte[] retValBytes0 = commState.getByteArray();

                if (retValBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                retValBytes = retValBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 40;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridNearAtomicUpdateResponse.class, this, "parent");
    }
}
