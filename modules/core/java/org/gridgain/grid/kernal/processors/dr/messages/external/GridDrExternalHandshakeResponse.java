/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.dr.messages.external;

import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;

/**
 * Response which is sent by receiving hub as result of {@link GridDrExternalHandshakeRequest} processing.
 *
 * @author @java.author
 * @version @java.version
 */
public class GridDrExternalHandshakeResponse implements GridDrExternalProtocolVersionAware, Externalizable {
    /** */
    private String protoVer;

    /** */
    private String marshClsName;

    /** */
    private Throwable err;

    /**
     * {@link Externalizable} support.
     */
    public GridDrExternalHandshakeResponse() {
        // No-op.
    }

    /**
     * @param protoVer DR protocol version.
     * @param marshClsName Marshaller class name.
     * @param err Handshake error or {@code null} if handshake succeeded.
     */
    public GridDrExternalHandshakeResponse(String protoVer, String marshClsName, @Nullable Throwable err) {
        this.protoVer = protoVer;
        this.marshClsName = marshClsName;
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public String protocolVersion() {
        return protoVer;
    }

    /**
     * @param err Handshake error or {@code null} if handshake succeeded.
     */
    public void error(@Nullable Throwable err) {
        this.err = err;
    }

    /**
     * @return Handshake error or {@code null} if handshake succeeded.
     */
    @Nullable public Throwable error() {
        return err;
    }

    /**
     * @return Marshaller class name.
     */
    public String marshallerClassName() {
        return marshClsName;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        U.writeString(out, protoVer);
        U.writeString(out, marshClsName);
        out.writeObject(err);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        protoVer = U.readString(in);
        marshClsName = U.readString(in);
        err = (Throwable)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDrExternalHandshakeResponse.class, this);
    }
}
