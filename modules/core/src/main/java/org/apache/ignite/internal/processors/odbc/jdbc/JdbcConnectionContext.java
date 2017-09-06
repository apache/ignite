package org.apache.ignite.internal.processors.odbc.jdbc;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.binary.BinaryReaderExImpl;
import org.apache.ignite.internal.processors.odbc.SqlListenerConnectionContext;
import org.apache.ignite.internal.processors.odbc.SqlListenerMessageParser;
import org.apache.ignite.internal.processors.odbc.SqlListenerProtocolVersion;
import org.apache.ignite.internal.processors.odbc.SqlListenerRequestHandler;
import org.apache.ignite.internal.util.GridSpinBusyLock;

/**
 * ODBC Connection Context.
 */
public class JdbcConnectionContext implements SqlListenerConnectionContext {
    /** Version 2.1.0. */
    private static final SqlListenerProtocolVersion VER_2_1_0 = SqlListenerProtocolVersion.create(2, 1, 0);

    /** Version 2.1.5: added "lazy" flag. */
    private static final SqlListenerProtocolVersion VER_2_1_5 = SqlListenerProtocolVersion.create(2, 1, 5);

    /** Current version. */
    private static final SqlListenerProtocolVersion CURRENT_VER = VER_2_1_5;

    /** Supported versions. */
    private static final Set<SqlListenerProtocolVersion> SUPPORTED_VERS = new HashSet<>();

    /** Context. */
    private final GridKernalContext ctx;

    /** Shutdown busy lock. */
    private final GridSpinBusyLock busyLock;

    /** Maximum allowed cursors. */
    private final int maxCursors;

    /** Message parser. */
    private JdbcMessageParser parser = null;

    /** Request handler. */
    private JdbcRequestHandler handler = null;

    static {
        SUPPORTED_VERS.add(CURRENT_VER);
        SUPPORTED_VERS.add(VER_2_1_0);
    }

    @Override
    public boolean isVersionSupported(SqlListenerProtocolVersion ver) {
        return SUPPORTED_VERS.contains(ver);
    }

    @Override
    public SqlListenerProtocolVersion currentVersion() {
        return CURRENT_VER;
    }

    @Override
    public void initFromHandshake(SqlListenerProtocolVersion ver, BinaryReaderExImpl reader) {
        assert SUPPORTED_VERS.contains(ver): "Unsupported JDBC protocol version.";

        boolean distributedJoins = reader.readBoolean();
        boolean enforceJoinOrder = reader.readBoolean();
        boolean collocated = reader.readBoolean();
        boolean replicatedOnly = reader.readBoolean();
        boolean autoCloseCursors = reader.readBoolean();

        boolean lazyExec = false;

        if (ver.compareTo(VER_2_1_5) >= 0)
            lazyExec = reader.readBoolean();

        handler = new JdbcRequestHandler(ctx, busyLock, maxCursors, distributedJoins,
                enforceJoinOrder, collocated, replicatedOnly, autoCloseCursors, lazyExec);

        parser = new JdbcMessageParser(ctx);
    }

    /**
     * Constructor.
     * @param ctx Kernal Context.
     * @param busyLock Shutdown busy lock.
     * @param maxCursors Maximum allowed cursors.
     */
    public JdbcConnectionContext(GridKernalContext ctx, GridSpinBusyLock busyLock, int maxCursors) {
        this.ctx = ctx;
        this.busyLock = busyLock;
        this.maxCursors = maxCursors;
    }

    @Override
    public SqlListenerRequestHandler handler() {
        return handler;
    }

    @Override
    public SqlListenerMessageParser parser() {
        return parser;
    }
}
