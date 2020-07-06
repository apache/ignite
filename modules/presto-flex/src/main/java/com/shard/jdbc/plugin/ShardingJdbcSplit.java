package com.shard.jdbc.plugin;

import static java.util.Objects.requireNonNull;

import java.util.Optional;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.predicate.TupleDomain;

public class ShardingJdbcSplit extends JdbcSplit {

	 	private final String connectorId;
	    private final String catalogName;
	    private final String schemaName;
	    private final String tableName;
	    private final TupleDomain<ColumnHandle> tupleDomain;

	    @JsonCreator
	    public ShardingJdbcSplit(
	            @JsonProperty("connectorId") String connectorId,
	            @JsonProperty("catalogName") @Nullable String catalogName,
	            @JsonProperty("schemaName") @Nullable String schemaName,
	            @JsonProperty("tableName") String tableName,
	            @JsonProperty("tupleDomain") TupleDomain<ColumnHandle> tupleDomain,
	            @JsonProperty("additionalProperty") Optional<String> additionalPredicate)
	    {
	    	super(additionalPredicate);
	        this.connectorId = requireNonNull(connectorId, "connector id is null");
	        this.catalogName = catalogName;
	        this.schemaName = schemaName;
	        this.tableName = requireNonNull(tableName, "table name is null");
	        this.tupleDomain = requireNonNull(tupleDomain, "tupleDomain is null");
	       
	    }

	    @JsonProperty
	    public String getConnectorId()
	    {
	        return connectorId;
	    }

	    @JsonProperty
	    @Nullable
	    public String getCatalogName()
	    {
	        return catalogName;
	    }

	    @JsonProperty
	    @Nullable
	    public String getSchemaName()
	    {
	        return schemaName;
	    }

	    @JsonProperty
	    public String getTableName()
	    {
	        return tableName;
	    }

	    @JsonProperty
	    public TupleDomain<ColumnHandle> getTupleDomain()
	    {
	        return tupleDomain;
	    }

	   
}
