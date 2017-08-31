package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteException;

/**
 * SQL Listener unsupported version exception.
 */
public class SqlListenerUnsupportedVersionException extends IgniteException {
    /** */
    private static final long serialVersionUID = 0L;

    /** Current version. */
    private final SqlListenerProtocolVersion currentVer;

    /**
     * Creates new exception with given error message.
     *
     * @param msg Error message.
     * @param currentVer Current version.
     */
    public SqlListenerUnsupportedVersionException(String msg, SqlListenerProtocolVersion currentVer) {
        super(msg);
        this.currentVer = currentVer;
    }

    /**
     * @return Current version.
     */
    public SqlListenerProtocolVersion getCurrentVersion() {
        return currentVer;
    }
}
