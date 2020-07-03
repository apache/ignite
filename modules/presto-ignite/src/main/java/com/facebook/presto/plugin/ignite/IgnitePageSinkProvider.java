package com.facebook.presto.plugin.ignite;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import javax.inject.Inject;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;

import com.facebook.presto.spi.ConnectorInsertTableHandle;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PageSinkProperties;
import com.facebook.presto.spi.connector.ConnectorPageSinkProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

public class IgnitePageSinkProvider implements ConnectorPageSinkProvider {

	private final JdbcClient jdbcClient;
	
    @Inject
    public IgnitePageSinkProvider(JdbcClient jdbcClient)
    {
    	this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorOutputTableHandle tableHandle, PageSinkProperties pageSinkProperties)
    {
        checkArgument(!pageSinkProperties.isPartitionCommitRequired(), "Jdbc connector does not support partition commit");
        return new IgnitePageSink(session, (JdbcOutputTableHandle) tableHandle, jdbcClient);
    }

    @Override
    public ConnectorPageSink createPageSink(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorInsertTableHandle tableHandle, PageSinkProperties pageSinkProperties)
    {
        checkArgument(!pageSinkProperties.isPartitionCommitRequired(), "Jdbc connector does not support partition commit");
        return new IgnitePageSink(session, (JdbcOutputTableHandle) tableHandle, jdbcClient);
    }
}
