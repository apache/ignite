/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.external;

import org.gridgain.grid.util.typedef.internal.*;

/**
 * Request which is sent by sending receiving hub to sender hub on connection in order to
 * identify sender hub data center ID.
 */
public class GridDrExternalHandshakeRequest {
    /** Data center ID. */
    private byte dataCenterId;

    /** Protocol version. */
    private String protoVer;

    /** Marshaller class name. */
    private String marshClsName;

    /**
     * Standard constructor.
     *
     * @param dataCenterId Data center ID.
     * @param protoVer Protocol version.
     * @param marshClsName Marshaller class name.
     */
    public GridDrExternalHandshakeRequest(byte dataCenterId, String protoVer, String marshClsName) {
        this.dataCenterId = dataCenterId;
        this.protoVer = protoVer;
        this.marshClsName = marshClsName;
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
     * @return Protocol version.
     */
    public String protocolVersion() {
        return protoVer;
    }

    /**
     * @return Marshaller class name.
     */
    public String marshallerClassName() {
        return marshClsName;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrExternalHandshakeRequest.class, this);
    }
}
