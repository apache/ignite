package database.ddl.transfer.factory.convert.impl;

import java.util.Map;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.factory.convert.BaseTypeConverter;

/**
 * Oracle至PostgreSQL的数据类型转换
 *
 * @author gs
 */
public class Oracle2PostgreSQLTypeConverter extends BaseTypeConverter {

	public Oracle2PostgreSQLTypeConverter(Map<String, String> typeMapping, Map<String, String> typeProperties) {
		super(typeMapping, typeProperties);
	}

	@Override
	public String convert(Column column) {
		return typeMapping.get(column.getColumnType().toUpperCase());
	}

}
