package org.apache.ignite.internal.processors.security;

/**
 *
 */
public class GridSecuritySessionImpl implements GridSecuritySession {
    /** Grid Security Manager. */
    private final GridSecurityManager mngr;

    /** Security context. */
    private final SecurityContext secCtx;

    /**
     * @param mngr Grid Security Manager.
     * @param secCtx Security context.
     */
    public GridSecuritySessionImpl(GridSecurityManager mngr, SecurityContext secCtx) {
        this.mngr = mngr;
        this.secCtx = secCtx;
    }

    /** {@inheritDoc} */
    @Override public void close() {
        mngr.startSession(secCtx);
    }
}
