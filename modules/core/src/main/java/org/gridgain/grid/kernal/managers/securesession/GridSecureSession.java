/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.securesession;

import org.gridgain.grid.spi.*;

import java.util.*;

/**
 * Secure session object.
 */
public class GridSecureSession {
    /** Subject type. */
    private GridSecuritySubjectType subjType;

    /** Subject. */
    private UUID subj;

    /** Authentication subject context returned by authentication SPI. */
    private Object authSubjCtx;

    /** Session creation time. */
    private byte[] sesTok;

    /**
     * @param subjType Subject type.
     * @param subj Subject.
     * @param authSubjCtx Authentication subject context.
     * @param sesTok Session token.
     */
    public GridSecureSession(GridSecuritySubjectType subjType, UUID subj, Object authSubjCtx, byte[] sesTok) {
        this.subjType = subjType;
        this.subj = subj;
        this.authSubjCtx = authSubjCtx;
        this.sesTok = sesTok;
    }

    /**
     * @return Subject type.
     */
    public GridSecuritySubjectType subjectType() {
        return subjType;
    }

    /**
     * @return Subject.
     */
    public UUID subject() {
        return subj;
    }

    /**
     * @return Authentication subject context returned by authentication SPI.
     */
    public Object authenticationSubjectContext() {
        return authSubjCtx;
    }

    /**
     * @return Session creation time.
     */
    public byte[] sessionToken() {
        return sesTok;
    }
}
