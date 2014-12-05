/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.plugin.security;

import org.gridgain.grid.*;

/**
 * Basic implementation for {@link GridSecurityCredentialsProvider}. Use it
 * when custom logic for storing security credentials is not required and it
 * is OK to specify credentials directly in configuration.
 */
public class GridSecurityCredentialsBasicProvider implements GridSecurityCredentialsProvider {
    /** */
    private GridSecurityCredentials cred;

    /**
     * Constructs security credentials provider based on security credentials passed in.
     *
     * @param cred Security credentials.
     */
    public GridSecurityCredentialsBasicProvider(GridSecurityCredentials cred) {
        this.cred = cred;
    }

    /** {@inheritDoc} */
    @Override public GridSecurityCredentials credentials() throws GridException {
        return cred;
    }
}
