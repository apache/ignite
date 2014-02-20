// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.direct.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.tostring.*;

import java.nio.*;
import java.util.*;

/**
 * Force keys response. Contains absent keys.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridReplicatedForceKeysResponse<K, V> extends GridCacheMessage<K, V> implements GridCacheDeployable {
    /** Future ID. */
    private GridUuid futId;

    /** Cache entries. */
    @GridToStringInclude
    @GridDirectTransient
    private List<GridCacheEntryInfo<K, V>> infos;

    /** */
    private byte[] infosBytes;

    /**
     * Required by {@link java.io.Externalizable}.
     */
    public GridReplicatedForceKeysResponse() {
        // No-op.
    }

    /**
     * @param futId Request id.
     */
    public GridReplicatedForceKeysResponse(GridUuid futId) {
        assert futId != null;

        this.futId = futId;
    }

    /** {@inheritDoc} */
    @Override public boolean allowForStartup() {
        return true;
    }

    /**
     * @return Forced entries.
     */
    public Collection<GridCacheEntryInfo<K, V>> forcedInfos() {
        return infos == null ? Collections.<GridCacheEntryInfo<K,V>>emptyList() : infos;
    }

    /**
     * @return Future ID.
     */
    public GridUuid futureId() {
        return futId;
    }

    /**
     * @param info Entry info to add.
     */
    public void addInfo(GridCacheEntryInfo<K, V> info) {
        assert info != null;

        if (infos == null)
            infos = new ArrayList<>();

        infos.add(info);
    }

    /** {@inheritDoc} */
    @Override public void prepareMarshal(GridCacheContext<K, V> ctx) throws GridException {
        super.prepareMarshal(ctx);

        if (infos != null) {
            marshalInfos(infos, ctx);

            infosBytes = ctx.marshaller().marshal(infos);
        }
    }

    /** {@inheritDoc} */
    @Override public void finishUnmarshal(GridCacheContext<K, V> ctx, ClassLoader ldr) throws GridException {
        super.finishUnmarshal(ctx, ldr);

        if (infosBytes != null) {
            infos = ctx.marshaller().unmarshal(infosBytes, ldr);

            unmarshalInfos(infos, ctx, ldr);
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"CloneDoesntCallSuperClone", "CloneCallsConstructors"})
    @Override public GridTcpCommunicationMessageAdapter clone() {
        GridReplicatedForceKeysResponse _clone = new GridReplicatedForceKeysResponse();

        clone0(_clone);

        return _clone;
    }

    /** {@inheritDoc} */
    @Override protected void clone0(GridTcpCommunicationMessageAdapter _msg) {
        super.clone0(_msg);

        GridReplicatedForceKeysResponse _clone = (GridReplicatedForceKeysResponse)_msg;

        _clone.futId = futId;
        _clone.infos = infos;
        _clone.infosBytes = infosBytes;
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
                if (!commState.putGridUuid(futId))
                    return false;

                commState.idx++;

            case 3:
                if (!commState.putByteArray(infosBytes))
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
                GridUuid futId0 = commState.getGridUuid();

                if (futId0 == GRID_UUID_NOT_READ)
                    return false;

                futId = futId0;

                commState.idx++;

            case 3:
                byte[] infosBytes0 = commState.getByteArray();

                if (infosBytes0 == BYTE_ARR_NOT_READ)
                    return false;

                infosBytes = infosBytes0;

                commState.idx++;

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public byte directType() {
        return 54;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridReplicatedForceKeysResponse.class, this, super.toString());
    }
}
