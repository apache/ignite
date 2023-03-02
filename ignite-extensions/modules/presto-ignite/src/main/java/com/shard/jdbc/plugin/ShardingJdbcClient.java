package com.shard.jdbc.plugin;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.*;
import io.prestosql.plugin.jdbc.credential.CredentialProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.shard.jdbc.database.DbInfo;
import com.shard.jdbc.exception.DbException;
import com.shard.jdbc.exception.NoMatchDataSourceException;
import com.shard.jdbc.shard.Shard;
import com.shard.jdbc.util.DbUtil;


import javax.annotation.Nullable;

import java.sql.Array;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.util.Locale.ENGLISH;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static com.google.common.collect.Iterables.getOnlyElement;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;

import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.CASE_INSENSITIVE_ORDER;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;


import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonFactoryBuilder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.prestosql.plugin.jdbc.expression.AggregateFunctionRule;
import io.prestosql.plugin.jdbc.expression.ImplementAvgDecimal;
import io.prestosql.plugin.jdbc.expression.ImplementAvgFloatingPoint;
import io.prestosql.plugin.jdbc.expression.ImplementCount;
import io.prestosql.plugin.jdbc.expression.ImplementCountAll;
import io.prestosql.plugin.jdbc.expression.ImplementMinMax;
import io.prestosql.plugin.jdbc.expression.ImplementSum;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.connector.AggregateFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;

import static com.fasterxml.jackson.core.JsonFactory.Feature.CANONICALIZE_FIELD_NAMES;
import static com.fasterxml.jackson.databind.SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedLongArray;
import static io.prestosql.plugin.jdbc.ColumnMapping.DISABLE_PUSHDOWN;
import static io.prestosql.plugin.jdbc.DecimalConfig.DecimalMapping.ALLOW_OVERFLOW;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalDefaultScale;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRounding;
import static io.prestosql.plugin.jdbc.DecimalSessionSessionProperties.getDecimalRoundingMode;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.decimalColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.*;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.fromPrestoTimestamp;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timeWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.timestampReadFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.prestosql.plugin.jdbc.UnsupportedTypeHandling.IGNORE;

import static io.prestosql.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.StandardTypes.JSON;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.util.Collections.addAll;

import static  com.shard.jdbc.util.TypeUtils.*;

public class ShardingJdbcClient extends BaseJdbcClient {
	public static final String PRIMARY_KEY = "primary_key";
	protected static final Logger log = Logger.get(ShardingJdbcClient.class);
	private static final int ARRAY_RESULT_SET_VALUE_COLUMN = 2;
	ShardingJdbcConfig igniteConfig;
	
	 private final Type jsonType;
	 private final Type uuidType;
	 private final MapType varcharMapType;

    @Inject
    public ShardingJdbcClient(BaseJdbcConfig config, ShardingJdbcConfig shardingConfig,ConnectionFactory connectionFactory,TypeManager typeManager) {
    	 super(config, shardingConfig.getIdentifierQuote(),connectionFactory);
    	 this.jsonType = typeManager.getType(new TypeSignature(JSON));
         this.uuidType = typeManager.getType(new TypeSignature(StandardTypes.UUID));
         this.varcharMapType = (MapType) typeManager.getType(mapType(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature()));
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


   
    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        return super.getTables(connection, schemaName, tableName);
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
   


    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        return new QueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                connection.getCatalog(),
                table.getSchemaName(),
                table.getTableName(),
                table.getGroupingSets(),
                columns,
                table.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
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
    
    private static ResultSet getPkColumns(JdbcTableHandle tableHandle, DatabaseMetaData metadata)
            throws SQLException {
        ResultSet columnSet = metadata.getPrimaryKeys(
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName()
                );
        return columnSet;
    }
  */ 
    

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

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnSql(session, column, columnName));
            }

            String sql = createTableSql(catalog, remoteSchema, tableName, columnList.build());
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
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns)
    {
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        JdbcIdentity identity = JdbcIdentity.from(session);

        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
        ImmutableList.Builder<JdbcTypeHandle> jdbcColumnTypes = ImmutableList.builder();
        
        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            String tableName = remoteTable;//generateTemporaryTableName();
            if (uppercase) {
                tableName = tableName.toUpperCase(ENGLISH);
            }
            String catalog = connection.getCatalog();

           
            for (JdbcColumnHandle column : columns) {
                columnNames.add(column.getColumnName());
                columnTypes.add(column.getColumnType());
                jdbcColumnTypes.add(column.getJdbcTypeHandle());
            }

            copyTableSchema(connection, catalog, remoteSchema, remoteTable, tableName, columnNames.build());

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    Optional.of(jdbcColumnTypes.build()),
                    tableName);
        }
        catch (PrestoException e) {
            //throw new PrestoException(JDBC_ERROR, e);        	
        	log.info("Begin insert table, Table already exists  for "+schemaTableName.getTableName());
        	
        	return new JdbcOutputTableHandle(                     
        			 null,
        			 schemaTableName.getSchemaName(),
        			 schemaTableName.getTableName(),
                     columnNames.build(),
                     columnTypes.build(),
                     Optional.of(jdbcColumnTypes.build()),
                     schemaTableName.getTableName());
        } catch (SQLException e) {
        	throw new PrestoException(JDBC_ERROR, e);
		}
    }
    
    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
    	log.info("commitCreateTable "+identity+" for "+handle.getTableName());
    }
    
    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
    	log.info("rollbackCreateTable "+identity+" for "+handle.getTableName());
    }
    
    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
    	log.info("finishInsertTable "+identity+" for "+handle.getTableName());
    }
    
    @Override
    public ConnectorSplitSource getSplits(ConnectorSession session, JdbcTableHandle tableHandle)
    {
      
    	List<JdbcSplit> list = new ArrayList<>();
    
    	TupleDomain<ColumnHandle> predicate = tableHandle.getConstraint();
    	
		Collection<DbInfo> dblist = DbUtil.getDataNodeListForType(tableHandle.getTableName());
		
		for(DbInfo dbInfo: dblist) {
			JdbcSplit jdbcSplit = new ShardingJdbcSplit(
					dbInfo.getId(),
                tableHandle.getCatalogName(),
                tableHandle.getSchemaName(),
                tableHandle.getTableName(),
                predicate,
                Optional.empty());
			
			list.add(jdbcSplit);
		}
        return new FixedSplitSource(list);
    }
    
    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        Connection connection = null;//connectionFactory.openConnection(identity);
        try {
        	ShardingJdbcSplit shardSplit = (ShardingJdbcSplit) split;
        	connection = DbUtil.getConnection(shardSplit.getConnectorId());
            connection.setReadOnly(true);
        }
        catch (SQLException e) {
            if(connection!=null) connection.close();
            throw e;
        } catch (DbException e) {
			// TODO Auto-generated catch block
        	throw new PrestoException(JDBC_ERROR, e);
		}
        return connection;
    }
    

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
    	Shard shard = new Shard(handle.getTableName(),"user",identity.getUser().hashCode());
    	Connection connection;
		try {
			//connection = DbUtil.getConnection(handle.getTableName(),shard);
			connection = connectionFactory.openConnection(identity);
			return connection;
		} catch (Exception e) {			
			throw new PrestoException(JDBC_ERROR, e);
		}    	
    }
    

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }
        switch (jdbcTypeName) {
            case "money":
                return Optional.of(moneyColumnMapping());
            case "uuid":
                return Optional.of(uuidColumnMapping());
           
            case "timestamptz":
                // PostgreSQL's "timestamp with time zone" is reported as Types.TIMESTAMP rather than Types.TIMESTAMP_WITH_TIMEZONE
                return Optional.of(timestampWithTimeZoneColumnMapping());
                
            case "hstore":
                return Optional.of(hstoreColumnMapping(session));
          
        }       
        if (typeHandle.getJdbcType() == Types.TIME) {
            return Optional.of(timeColumnMapping(session));
        }
        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {        	
        	
            return Optional.of(timestampColumnMappingUsingSqlTimestamp(session));
        }
        if (typeHandle.getJdbcType() == Types.NUMERIC && getDecimalRounding(session) == ALLOW_OVERFLOW) {
            if (typeHandle.getColumnSize() == 131089) {
                // decimal type with unspecified scale - up to 131072 digits before the decimal point; up to 16383 digits after the decimal point)
                // 131089 = SELECT LENGTH(pow(10::numeric,131071)::varchar); 131071 = 2^17-1  (org.postgresql.jdbc.TypeInfoCache#getDisplaySize)
                return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, getDecimalDefaultScale(session)), getDecimalRoundingMode(session)));
            }
            int precision = typeHandle.getColumnSize();
            if (precision > Decimals.MAX_PRECISION) {
                int scale = min(typeHandle.getDecimalDigits(), getDecimalDefaultScale(session));
                return Optional.of(decimalColumnMapping(createDecimalType(Decimals.MAX_PRECISION, scale), getDecimalRoundingMode(session)));
            }
        }
        if (typeHandle.getJdbcType() == Types.ARRAY) {
            
            // resolve and map base array element type
            JdbcTypeHandle baseElementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
            String baseElementTypeName = baseElementTypeHandle.getJdbcTypeName()
                    .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Element type name is missing: " + baseElementTypeHandle));
            if (baseElementTypeHandle.getJdbcType() == Types.VARBINARY) {
                // PostgreSQL jdbc driver doesn't currently support array of varbinary (bytea[])
                // https://github.com/pgjdbc/pgjdbc/pull/1184
                return Optional.empty();
            }
            Optional<ColumnMapping> baseElementMapping = toPrestoType(session, connection, baseElementTypeHandle);

            if (typeHandle.getArrayDimensions().isEmpty()) {
                return Optional.empty();
            }
            return baseElementMapping
                    .map(elementMapping -> {
                        ArrayType prestoArrayType = new ArrayType(elementMapping.getType());
                        ColumnMapping arrayColumnMapping = arrayColumnMapping(session, prestoArrayType, elementMapping, baseElementTypeName);

                        int arrayDimensions = typeHandle.getArrayDimensions().get();
                        for (int i = 1; i < arrayDimensions; i++) {
                            prestoArrayType = new ArrayType(prestoArrayType);
                            arrayColumnMapping = arrayColumnMapping(session, prestoArrayType, arrayColumnMapping, baseElementTypeName);
                        }
                        return arrayColumnMapping;
                    });
           
        }
        // TODO support PostgreSQL's TIME WITH TIME ZONE explicitly, otherwise predicate pushdown for these types may be incorrect
        return super.toPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }
        if (TIME.equals(type)) {
            return WriteMapping.longMapping("time", timeWriteFunction(session));
        }
        if (TIMESTAMP.equals(type)) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunctionUsingSqlTimestamp(session));
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.longMapping("timestamptz", timestampWithTimeZoneWriteFunction());
        }
        if (TinyintType.TINYINT.equals(type)) {
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
       
        if (type.equals(uuidType)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }
        if (type instanceof ArrayType) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }
        return super.toWriteMapping(session, type);
    }

   

    private static ColumnMapping timestampWithTimeZoneColumnMapping()
    {
        return ColumnMapping.longMapping(
                TIMESTAMP_WITH_TIME_ZONE,
                (resultSet, columnIndex) -> {
                    // PostgreSQL does not store zone information in "timestamp with time zone" data type
                    long millisUtc = resultSet.getTimestamp(columnIndex).getTime();
                    return packDateTimeWithZone(millisUtc, UTC_KEY);
                },
                timestampWithTimeZoneWriteFunction());
    }

    private static LongWriteFunction timestampWithTimeZoneWriteFunction()
    {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new Timestamp(millisUtc));
        };
    }


    private ColumnMapping hstoreColumnMapping(ConnectorSession session)
    {
        return ColumnMapping.objectMapping(
                varcharMapType,
                varcharMapReadFunction(),
                hstoreWriteFunction(session),
                DISABLE_PUSHDOWN);
    }

    private ObjectReadFunction varcharMapReadFunction()
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            @SuppressWarnings("unchecked")
            Map<String, String> map = (Map<String, String>) resultSet.getObject(columnIndex);
            BlockBuilder keyBlockBuilder = varcharMapType.getKeyType().createBlockBuilder(null, map.size());
            BlockBuilder valueBlockBuilder = varcharMapType.getValueType().createBlockBuilder(null, map.size());
            for (Map.Entry<String, String> entry : map.entrySet()) {
                if (entry.getKey() == null) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "hstore key is null");
                }
                varcharMapType.getKeyType().writeSlice(keyBlockBuilder, utf8Slice(entry.getKey()));
                if (entry.getValue() == null) {
                    valueBlockBuilder.appendNull();
                }
                else {
                    varcharMapType.getValueType().writeSlice(valueBlockBuilder, utf8Slice(entry.getValue()));
                }
            }
            return varcharMapType.createBlockFromKeyValue(Optional.empty(), new int[] {0, map.size()}, keyBlockBuilder.build(), valueBlockBuilder.build())
                    .getObject(0, Block.class);
        });
    }

    private ObjectWriteFunction hstoreWriteFunction(ConnectorSession session)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            checkArgument(block instanceof SingleMapBlock, "wrong block type: %s. expected SingleMapBlock", block.getClass().getSimpleName());
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < block.getPositionCount(); i += 2) {
                map.put(varcharMapType.getKeyType().getObjectValue(session, block, i), varcharMapType.getValueType().getObjectValue(session, block, i + 1));
            }
            statement.setObject(index, Collections.unmodifiableMap(map));
        });
    }

   

    private static ColumnMapping arrayColumnMapping(ConnectorSession session, ArrayType arrayType, ColumnMapping arrayElementMapping, String baseElementJdbcTypeName)
    {
        return ColumnMapping.objectMapping(
                arrayType,
                arrayReadFunction(arrayType.getElementType(), arrayElementMapping.getReadFunction()),
                arrayWriteFunction(session, arrayType.getElementType(), baseElementJdbcTypeName));
    }

    private static ObjectReadFunction arrayReadFunction(Type elementType, ReadFunction elementReadFunction)
    {
        return ObjectReadFunction.of(Block.class, (resultSet, columnIndex) -> {
            Array array = resultSet.getArray(columnIndex);
            BlockBuilder builder = elementType.createBlockBuilder(null, 10);
            try (ResultSet arrayAsResultSet = array.getResultSet()) {
                while (arrayAsResultSet.next()) {
                    if (elementReadFunction.isNull(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN)) {
                        builder.appendNull();
                    }
                    else if (elementType.getJavaType() == boolean.class) {
                        elementType.writeBoolean(builder, ((BooleanReadFunction) elementReadFunction).readBoolean(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == long.class) {
                        elementType.writeLong(builder, ((LongReadFunction) elementReadFunction).readLong(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == double.class) {
                        elementType.writeDouble(builder, ((DoubleReadFunction) elementReadFunction).readDouble(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else if (elementType.getJavaType() == Slice.class) {
                        elementType.writeSlice(builder, ((SliceReadFunction) elementReadFunction).readSlice(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                    else {
                        elementType.writeObject(builder, ((ObjectReadFunction) elementReadFunction).readObject(arrayAsResultSet, ARRAY_RESULT_SET_VALUE_COLUMN));
                    }
                }
            }

            return builder.build();
        });
    }

    private static ObjectWriteFunction arrayWriteFunction(ConnectorSession session, Type elementType, String baseElementJdbcTypeName)
    {
        return ObjectWriteFunction.of(Block.class, (statement, index, block) -> {
            Array jdbcArray = statement.getConnection().createArrayOf(baseElementJdbcTypeName, getJdbcObjectArray(session, elementType, block));
            statement.setArray(index, jdbcArray);
        });
    }


    private static JdbcTypeHandle getArrayElementTypeHandle(Connection connection, JdbcTypeHandle arrayTypeHandle)
    {
        String jdbcTypeName = arrayTypeHandle.getJdbcTypeName()
                .orElseThrow(() -> new PrestoException(JDBC_ERROR, "Type name is missing: " + arrayTypeHandle));
        try {
        	
            return new JdbcTypeHandle(
            		arrayTypeHandle.getJdbcType(),
            		Optional.of(jdbcTypeName),
                    arrayTypeHandle.getColumnSize(),
                    arrayTypeHandle.getDecimalDigits(),
                    arrayTypeHandle.getArrayDimensions());
        }
        catch (Exception e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }
    
    private static ColumnMapping moneyColumnMapping()
    {
        /*
         * PostgreSQL JDBC maps "money" to Types.DOUBLE, but fails to retrieve double value for amounts
         * greater than or equal to 1000. Upon `ResultSet#getString`, the driver returns e.g. "$10.00" or "$10,000.00"
         * (currency symbol depends on the server side configuration).
         *
         * The following mapping maps PostgreSQL "money" to Presto "varchar".
         * Writing is disabled for simplicity.
         *
         * Money mapping can be improved when PostgreSQL JDBC gains explicit money type support.
         * See https://github.com/pgjdbc/pgjdbc/issues/425 for more information.
         */
        return ColumnMapping.sliceMapping(
                VARCHAR,
                new SliceReadFunction()
                {
                    @Override
                    public boolean isNull(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        // super calls ResultSet#getObject(), which for money type calls .getDouble and the call may fail to parse the money value.
                        resultSet.getString(columnIndex);
                        return resultSet.wasNull();
                    }

                    @Override
                    public Slice readSlice(ResultSet resultSet, int columnIndex)
                            throws SQLException
                    {
                        return utf8Slice(resultSet.getString(columnIndex));
                    }
                },
                (statement, index, value) -> { throw new PrestoException(NOT_SUPPORTED, "Money type is not supported for INSERT"); },
                DISABLE_PUSHDOWN);
    }

    private static SliceWriteFunction uuidWriteFunction()
    {
        return (statement, index, value) -> {
            UUID uuid = new UUID(value.getLong(0), value.getLong(SIZE_OF_LONG));
            statement.setObject(index, uuid, Types.OTHER);
        };
    }

    private static Slice uuidSlice(UUID uuid)
    {
        return wrappedLongArray(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    private ColumnMapping uuidColumnMapping()
    {
        return ColumnMapping.sliceMapping(
                uuidType,
                (resultSet, columnIndex) -> uuidSlice((UUID) resultSet.getObject(columnIndex)),
                uuidWriteFunction());
    }
}
