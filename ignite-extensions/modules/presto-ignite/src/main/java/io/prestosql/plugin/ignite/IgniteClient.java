package io.prestosql.plugin.ignite;

import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.*;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.shard.jdbc.plugin.ShardingJdbcClient;
import com.shard.jdbc.plugin.ShardingJdbcConfig;
import com.shard.jdbc.plugin.ShardingJdbcSplit;

import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.IgniteJdbcDriver;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Locale.ENGLISH;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;


public class IgniteClient extends ShardingJdbcClient {

	IgniteConfig igniteConfig;

    @Inject
    public IgniteClient(BaseJdbcConfig config, ShardingJdbcConfig shardingConfig, IgniteConfig igniteConfig,ConnectionFactory connectionFactory,TypeManager typeManager) {
    	 super(config, shardingConfig, connectionFactory,typeManager);       		
    	 this.igniteConfig = igniteConfig;
    }

    @Override
    protected Collection<String> listSchemas(Connection connection){
        // for MySQL, we need to list catalogs instead of schemas
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1); //.toLowerCase(ENGLISH);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
   

    protected String getTableWithString(ConnectorTableMetadata tableMetadata, String tableName)
    {    	
    	if(tableMetadata.getProperties().size()>0) {    		
    		StringBuilder with = new StringBuilder();
    		with.append("WITH ");
    		with.append('"');    		
    		for(Map.Entry<String,Object> ent: tableMetadata.getProperties().entrySet()) {    			
    			if(ent.getKey().equalsIgnoreCase(PRIMARY_KEY)) continue;
    			with.append(ent.getKey());
    			with.append('=');
    			with.append(ent.getValue());
    			with.append(',');
    		}
    		with.append('"');
    		return with.length()>8 ? with.toString():"";
    	}
    	return "";
    }
   


  /**

    private static ResultSet getColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException {
        ResultSet columnSet = metadata.getColumns(
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                null);
        return columnSet;
    }
    */
    
    
    public List<String> getPkColumns(Connection connection, SchemaTableName tableHandle)
    {
    	List<String> columns = new ArrayList<>();    	
    	try (ResultSet resultSet = getPkColumns(tableHandle, connection.getMetaData())) {
            
            while (resultSet.next()) {
            	String columnName = resultSet.getString("COLUMN_NAME");                    
            	columns.add(columnName);
            }        	
            return ImmutableList.copyOf(columns);
        }
    	catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    
    private static ResultSet getPkColumns(SchemaTableName tableHandle, DatabaseMetaData metadata)
            throws SQLException {
        ResultSet columnSet = metadata.getPrimaryKeys(
        		null,
                tableHandle.getSchemaName(),
                tableHandle.getTableName()
                );
        return columnSet;
    }
  

    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
            throws SQLException
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(identity).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

            List<String> pkColnums = getPkColumns(connection,tableMetadata.getTable());
            
            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            
            //add@byron
            StringBuilder primary_keys = new StringBuilder();
            boolean hasPk = false;
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnSql(session, column, columnName));
                
                Object isPK = column.getProperties().get(PRIMARY_KEY);                
                if(isPK!=null || pkColnums.indexOf(columnName)>=0) {
                	if(hasPk) primary_keys.append(',');
                	primary_keys.append(columnName);
                	hasPk = true;
                }
                
            }
            if(tableMetadata.getProperties().containsKey(PRIMARY_KEY)) {
            	String columnName = tableMetadata.getProperties().get(PRIMARY_KEY).toString();
            	if(columnName.length()< 255) { //for sql safe
            		primary_keys.setLength(0);
            		primary_keys.append(columnName);
            		hasPk = true;
            	}
            	else {
            		 throw new PrestoException(JDBC_ERROR, "With primary key content is too long!");
            	}
            }
            if(hasPk) {
            	primary_keys.append(" )\n ");
            	columnList.add("\n\tPRIMARY KEY ( "+primary_keys.toString());
            }
            //end@byron
            
            String sql = createTableSql(catalog, remoteSchema, tableName, columnList.build());
            sql += getTableWithString(tableMetadata,tableName);
            
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.empty(),
                    tableName);
        }
    }


    private String getColumnSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder()
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }        
        //add@byron
        if(column.getExtraInfo()!=null) {
        	sb.append(' ');
    		sb.append(column.getExtraInfo());
        }
        //end@
        return sb.toString();
    }

    
    protected String createTableSql(String catalog, String remoteSchema, String tableName, List<String> columns)
    {
        return format("CREATE TABLE %s (%s)", quoted(catalog, remoteSchema, tableName), join(", ", columns));
    }
    
    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
    	try {
            return createTable(session, tableMetadata,tableMetadata.getTable().getTableName());
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
    


    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
    	JdbcSplit jdbcSplit = new ShardingJdbcSplit(
				"",
            tableHandle.getCatalogName(),
            tableHandle.getSchemaName(),
            tableHandle.getTableName(),
            tableHandle.getConstraint(),
            Optional.empty());
		
        return new FixedSplitSource(ImmutableList.of(jdbcSplit));
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        try {
            connection.setReadOnly(true);
        }
        catch (SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }
}
