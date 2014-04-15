/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest.client.message;

import java.io.*;

/**
 * Client authentication request.
 */
public class GridClientAuthenticationRequest extends GridClientAbstractMessage {
    private static final long serialVersionUID = 0L;

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
}
