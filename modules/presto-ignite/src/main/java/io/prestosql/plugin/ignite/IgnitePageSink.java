package io.prestosql.plugin.ignite;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static io.prestosql.plugin.ignite.TypeUtils.isArrayType;
import static io.prestosql.plugin.ignite.TypeUtils.isMapType;
import static io.prestosql.plugin.ignite.TypeUtils.isRowType;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.joda.time.chrono.ISOChronology.getInstanceUTC;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.joda.time.DateTimeZone;

import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcPageSink;

import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeWithTimeZoneType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignatureParameter;
import io.prestosql.spi.type.VarbinaryType;


public class IgnitePageSink extends JdbcPageSink {

	private final String implicitPrefix = "_pos";
	
	

	public IgnitePageSink(ConnectorSession session, JdbcOutputTableHandle handle, JdbcClient jdbcClient) {
		super(session, handle, jdbcClient);
	}

	
   
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

			Block arrayBlock = block.getSingleValueBlock(position);

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

			Block mapBlock = block.getSingleValueBlock(position);

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
			Block rowBlock = block.getSingleValueBlock(position);

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
