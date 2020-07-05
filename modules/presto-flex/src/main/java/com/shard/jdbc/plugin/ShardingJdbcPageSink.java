package com.shard.jdbc.plugin;

import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static com.facebook.presto.plugin.jdbc.JdbcErrorCode.JDBC_NON_TRANSIENT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.shard.jdbc.util.TypeUtils.isArrayType;
import static com.shard.jdbc.util.TypeUtils.isMapType;
import static com.shard.jdbc.util.TypeUtils.isRowType;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTimeZone;

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcOutputTableHandle;
import com.facebook.presto.plugin.jdbc.JdbcPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateTimeEncoding;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.facebook.presto.spi.type.VarbinaryType;
import com.google.common.collect.ImmutableList;
import com.shard.jdbc.database.DbInfo;
import com.shard.jdbc.exception.DbException;
import com.shard.jdbc.exception.NoMatchDataSourceException;
import com.shard.jdbc.shard.Shard;
import com.shard.jdbc.shard.ShardProperty;
import com.shard.jdbc.util.DbUtil;

import io.airlift.slice.Slice;


public class ShardingJdbcPageSink extends JdbcPageSink {

	private final String implicitPrefix = "_pos";
	protected final PreparedStatement[] statements;
	protected final Connection[] connections;
	
	protected final JdbcOutputTableHandle handle;

	public ShardingJdbcPageSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient) {
		super(session, handle, jdbcClient);		
		this.handle = handle;
		
		Collection<DbInfo> list = DbUtil.getDataNodeListForType(handle.getTableName());
		this.statements = new PreparedStatement[list.size()];
		this.connections = new Connection[list.size()];
		String insertSql = jdbcClient.buildInsertSql(handle);
		Iterator<DbInfo> it = list.iterator();
		for(int i=0;i<list.size();i++) {
			try {
				connections[i] = DbUtil.getConnection(it.next().getId());
	            connections[i].setAutoCommit(false);	            
	            statements[i] = connections[i].prepareStatement(insertSql);
	        }
	        catch (SQLException | NoMatchDataSourceException e) {		          
	            throw new PrestoException(JDBC_ERROR, e);
	        }
		}
	}
	
	@Override
    public CompletableFuture<?> appendPage(Page page)
    {
		
        try {
        	
        	ShardProperty shardP = DbUtil.getShardPropertyForType(handle.getTableName());
        	
            for (int position = 0; position < page.getPositionCount(); position++) {
            	Object[] row = new Object[page.getChannelCount()];
            	PreparedStatement statement = this.statement;// primary meta statement
            	Connection connection = this.connection; //primary meta connect
            	
                for (int channel = 0; channel < page.getChannelCount(); channel++) {
                	
                	String column = handle.getColumnNames().get(channel);
                	
                	 Block block = page.getBlock(channel);
                     int parameter = channel + 1;

                     Type type = this.columnTypes.get(channel);                   
                     row[channel] = getObjectValue(type, block, position);
                     
                     
                     if(column.equalsIgnoreCase(shardP.getColumn())) {
                    	 Shard shard = new Shard(handle.getTableName(),shardP.getColumn(),row[channel].hashCode());
                    	 
                    	 connection = DbUtil.getConnection(handle.getTableName(),shard);
                    	 int find = -1;
                    	 for(int index=0;index<connections.length;index++) {
                    		 if(connection==connections[index]) {
                    			 find = index;
                    			 statement = statements[index];
                    			 
                    			 break;
                    		 }
                    	 }
                    	 
                     }
                     
                     for(int i=0;i<row.length;i++) {
                    	 statement.setObject(i+1, row[i]);
                     }
                }
                
              
                statement.addBatch();
                batchSize++;

                if (batchSize >= 1000) {
                    statement.executeBatch();
                    connection.commit();
                    connection.setAutoCommit(false);
                    batchSize = 0;
                }
            }
        }
        catch (SQLException | DbException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        return NOT_BLOCKED;
    }


    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
    	for(int i=0;i<this.connections.length;i++) {
	        // commit and close
	        try (Connection connection = this.connections[i];
	                PreparedStatement statement = this.statements[i]) {
	            if (batchSize > 0) {
	                statement.executeBatch();
	                connection.commit();
	            }
	        }
	        catch (SQLNonTransientException e) {
	            throw new PrestoException(JDBC_NON_TRANSIENT_ERROR, e);
	        }
	        catch (SQLException e) {
	            throw new PrestoException(JDBC_ERROR, e);
	        }
    	}
        // the committer does not need any additional info
        return super.finish();
    }

    @SuppressWarnings("unused")
    @Override
    public void abort()
    {
    	for(int i=0;i<this.connections.length;i++) {
	        // rollback and close
	        try (Connection connection = this.connections[i];
	                PreparedStatement statement = this.statements[i]) {
	            connection.rollback();
	        }
	        catch (SQLException e) {
	            // Exceptions happened during abort do not cause any real damage so ignore them
	            log.debug(e, "SQLException when abort");
	        }
    	}
    	super.abort();
    }
    
	@Override
	protected Object getObjectValue(Type type, Block block, int position) {
		if (block.isNull(position)) {
			return null;
		}
		if (type.equals(BooleanType.BOOLEAN)) {
			return type.getBoolean(block, position);
		}
		if (type.equals(BigintType.BIGINT)) {
			return type.getLong(block, position);
		}
		if (type.equals(IntegerType.INTEGER)) {
			return (int) type.getLong(block, position);
		}
		if (type.equals(SmallintType.SMALLINT)) {
			return (short) type.getLong(block, position);
		}
		if (type.equals(TinyintType.TINYINT)) {
			return (byte) type.getLong(block, position);
		}
		if (type.equals(DoubleType.DOUBLE)) {
			return type.getDouble(block, position);
		}
		if (isVarcharType(type)) {
			return type.getSlice(block, position).toStringUtf8();
		}
		if (type.equals(VarbinaryType.VARBINARY)) {
			return type.getSlice(block, position).getBytes();
		}
		if (type.equals(DateType.DATE)) {
			long days = type.getLong(block, position);
			return new Date(TimeUnit.DAYS.toMillis(days));
		}
		if (type.equals(TimeType.TIME)) {
			long millisUtc = type.getLong(block, position);
			return new Date(millisUtc);
		}
		if (type.equals(TimestampType.TIMESTAMP)) {
			long millisUtc = type.getLong(block, position);
			return new Date(millisUtc);
		}
		if (type.equals(TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE)) {
			long millisUtc = unpackMillisUtc(type.getLong(block, position));
			return new Date(millisUtc);
		}
		if (type instanceof DecimalType) {
			// TODO: decimal type might not support yet
			// TODO: this code is likely wrong and should switch to
			// Decimals.readBigDecimal()
			DecimalType decimalType = (DecimalType) type;
			BigInteger unscaledValue;
			if (decimalType.isShort()) {
				unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
			} else {
				unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
			}
			return new BigDecimal(unscaledValue);
		}
		if (isArrayType(type)) {
			Type elementType = type.getTypeParameters().get(0);

			Block arrayBlock = block.getBlock(position);

			List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
			for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
				Object element = getObjectValue(elementType, arrayBlock, i);
				list.add(element);
			}

			return unmodifiableList(list);
		}
		if (isMapType(type)) {
			Type keyType = type.getTypeParameters().get(0);
			Type valueType = type.getTypeParameters().get(1);

			Block mapBlock = block.getBlock(position);

			// map type is converted into list of fixed keys document
			List<Object> values = new ArrayList<>(mapBlock.getPositionCount() / 2);
			for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
				Map<String, Object> mapValue = new HashMap<>();
				mapValue.put("key", getObjectValue(keyType, mapBlock, i));
				mapValue.put("value", getObjectValue(valueType, mapBlock, i + 1));
				values.add(mapValue);
			}

			return unmodifiableList(values);
		}
		if (isRowType(type)) {
			Block rowBlock = block.getBlock(position);

			List<Type> fieldTypes = type.getTypeParameters();
			if (fieldTypes.size() != rowBlock.getPositionCount()) {
				throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
						"Expected row value field count does not match type field count");
			}

			if (isImplicitRowType(type)) {
				List<Object> rowValue = new ArrayList<>();
				for (int i = 0; i < rowBlock.getPositionCount(); i++) {
					Object element = getObjectValue(fieldTypes.get(i), rowBlock, i);
					rowValue.add(element);
				}
				return unmodifiableList(rowValue);
			}

			Map<String, Object> rowValue = new HashMap<>();
			for (int i = 0; i < rowBlock.getPositionCount(); i++) {
				rowValue.put(type.getTypeSignature().getParameters().get(i).getNamedTypeSignature().getName()
						.orElse("field" + i), getObjectValue(fieldTypes.get(i), rowBlock, i));
			}
			return unmodifiableMap(rowValue);
		}

		throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
	}

	private boolean isImplicitRowType(Type type) {
		return type.getTypeSignature().getParameters().stream().map(TypeSignatureParameter::getNamedTypeSignature)
				.map(NamedTypeSignature::getName).filter(Optional::isPresent).map(Optional::get)
				.allMatch(name -> name.startsWith(implicitPrefix));
	}
}
