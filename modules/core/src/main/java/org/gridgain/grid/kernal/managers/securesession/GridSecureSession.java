/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.securesession;

import org.gridgain.grid.kernal.managers.security.*;

/**
 * Secure session object.
 */
public class GridSecureSession {
    /** Authentication subject context returned by authentication SPI. */
    private GridSecurityContext authSubjCtx;

    /** Session creation time. */
    private byte[] sesTok;

    /**
     * @param authSubjCtx Authentication subject context.
     * @param sesTok Session token.
     */
    public GridSecureSession(GridSecurityContext authSubjCtx, byte[] sesTok) {
        this.authSubjCtx = authSubjCtx;
        this.sesTok = sesTok;
    }

    /**
     * @return Authentication subject context returned by authentication SPI.
     */
    public GridSecurityContext authenticationSubjectContext() {
        return authSubjCtx;
    }

    /**
     * @return Session creation time.
     */
    public byte[] sessionToken() {
        return sesTok;
    }
}
