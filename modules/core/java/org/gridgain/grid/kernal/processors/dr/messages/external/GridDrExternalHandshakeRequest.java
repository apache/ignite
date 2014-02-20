// @java.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.external;

import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Request which is sent by sending receiving hub to sender hub on connection in order to
 * identify sender hub data center ID.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDrExternalHandshakeRequest implements GridDrExternalProtocolVersionAware, Externalizable {
    /** Data center ID. */
    private byte dataCenterId;

    /** DR protocol version. */
    private String protoVer;

    /** Marshaller class name. */
    private String marshClsName;

    /**
     * {@link Externalizable} support.
     */
    public GridDrExternalHandshakeRequest() {
        // No-op.
    }

    /**
     * Standard constructor.
     *
     * @param dataCenterId Data center ID.
     * @param protoVer DR protocol version.
     * @param marshClsName Marshaller class name.
     */
    public GridDrExternalHandshakeRequest(byte dataCenterId, String protoVer, String marshClsName) {
        this.dataCenterId = dataCenterId;
        this.protoVer = protoVer;
        this.marshClsName = marshClsName;
    }

    /** {@inheritDoc} */
    @Override public String protocolVersion() {
        return protoVer;
    }

    /**
     * Get data center ID.
     *
     * @return Data center ID.
     */
    public byte dataCenterId() {
        return dataCenterId;
    }

    /**
     * @return Marshaller class name.
     */
    public String marshallerClassName() {
        return marshClsName;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeByte(dataCenterId);
        U.writeString(out, protoVer);
        U.writeString(out, marshClsName);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dataCenterId = in.readByte();
        protoVer = U.readString(in);
        marshClsName = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrExternalHandshakeRequest.class, this);
    }
}
