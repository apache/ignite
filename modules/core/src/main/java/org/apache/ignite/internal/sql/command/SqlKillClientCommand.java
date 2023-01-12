package org.apache.ignite.internal.sql.command;

import org.apache.ignite.internal.sql.SqlLexer;
import org.apache.ignite.internal.sql.SqlLexerTokenType;
import org.apache.ignite.internal.sql.SqlParserUtils;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;

/**
 * KILL CLIENT command.
 *
 * @see org.apache.ignite.internal.visor.client.VisorClientConnectionDropTask
 * @see ClientConnectionView#connectionId()
 */
public class SqlKillClientCommand implements SqlCommand {
    /** Connections id. */
    private Long connectionId;

    /** {@inheritDoc} */
    @Override public SqlCommand parse(SqlLexer lex) {
        if (lex.shift()) {
            if (lex.tokenType() == SqlLexerTokenType.DEFAULT) {
                String connIdStr = lex.token();

                if (!connIdStr.equals("ALL"))
                    connectionId = Long.parseLong(connIdStr);

                return this;
            }
        }

        throw SqlParserUtils.error(lex, "Expected client connection id or ALL.");
    }

    /** {@inheritDoc} */
    @Override public String schemaName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void schemaName(String schemaName) {
        // No-op.
    }

    /** @return Connection id to drop. */
    public Long connectionId() {
        return connectionId;
    }
}
