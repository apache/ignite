package io.prestosql.plugin.ignite;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import javax.inject.Inject;

import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTransactionHandle;

public class IgnitePageSinkProvider implements ConnectorPageSinkProvider {

	private final JdbcClient jdbcClient;
	
    @Inject
    public IgnitePageSinkProvider(JdbcClient jdbcClient)
    {
    	this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle)
    {
        
        return new IgnitePageSink(session, (JdbcOutputTableHandle) tableHandle, jdbcClient);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle)
    {       
        return new IgnitePageSink(session, (JdbcOutputTableHandle) tableHandle, jdbcClient);
    }
}
