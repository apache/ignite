/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import org.gridgain.grid.portable.*;

import java.io.*;

/**
 * Client authentication request.
 */
public class GridClientAuthenticationRequest extends GridClientAbstractMessage {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    public static final int PORTABLE_TYPE_ID = nextSystemTypeId();

    /** Credentials. */
    private Object cred;

    /**
     * @return Credentials object.
     */
    public Object credentials() {
        return cred;
    }

    /**
     * @param cred Credentials object.
     */
    public void credentials(Object cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(cred);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        cred = in.readObject();
    }

    /** {@inheritDoc} */
    @Override public int typeId() {
        return PORTABLE_TYPE_ID;
    }

    /** {@inheritDoc} */
    @Override public void writePortable(GridPortableWriter writer) throws IOException {
        super.writePortable(writer);

        writer.writeObject("cred", cred);
    }

    /** {@inheritDoc} */
    @Override public void readPortable(GridPortableReader reader) throws IOException {
        super.readPortable(reader);

        cred = reader.readObject("cred");
    }
}
