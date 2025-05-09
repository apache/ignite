/*********************************************************************************
 * Copyright (c)2020 CEC Health
 * FILE: TypeConverter
 * 版本      DATE             BY               REMARKS
 * ----  -----------  ---------------  ------------------------------------------
 * 1.0   2020-01-14        luoyuntian
 ********************************************************************************/
package database.ddl.transfer.factory.convert;

import database.ddl.transfer.bean.DataBaseDefine;
import database.ddl.transfer.bean.Table;
import database.ddl.transfer.consts.DataBaseType;
import database.ddl.transfer.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import database.ddl.transfer.bean.Column;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 转换数据类型类
 *
 * @author gs
 */
public abstract class BaseTypeConverter {

	protected Logger logger = LoggerFactory.getLogger(this.getClass().getName());

	/**
	 * 数据类型映射
	 */
	protected Map<String, String> typeMapping;

	protected Map<String, String> typeProperties;

	public BaseTypeConverter(Map<String, String> typeMapping, Map<String, String> typeProperties) {
		this.typeMapping = typeMapping;
		this.typeProperties = typeProperties;
	}

	/**
	 * 转换数据类型
	 * 
	 * @param column 数据列定义
	 * @return 转换后的，符合目标数据库要求的数据类型
	 */
	public abstract String convert(Column column);

	/**
	 * @author luoyuntian
	 * @date 2019-12-30 14:46
	 * @description 将DataBaseDefine转换成对应的数据类型
	 * @param dataBaseDefine
	 * @return
	 */
	public DataBaseDefine convert(DataBaseDefine dataBaseDefine) {
		Collection<Table> tables = dataBaseDefine.getTablesMap().values();
		for (Table table : tables) {
			List<Column> columns = table.getColumns();
			for (Column column : columns) {
				String columnType = typeMapping.get(column.getDataType().toUpperCase());
				StringBuilder stringBuilder = new StringBuilder("");
				if (typeMapping.containsKey(column.getDataType().toUpperCase())) {
					// 配置了映射关系
					if (!typeProperties.containsKey(columnType)) {
						// 不加精度和长度
						column.setFinalConvertDataType(columnType);
					} else {
						// 加精度和长度
						if (!StringUtil.isBlank(columnType) && columnType.contains("(")) {
							// 自带默认长度或精度
							stringBuilder.append(columnType);
						} else {
							// 转换时不自带长度或精度，需要重新获取拼接
							stringBuilder.append(columnType);
							if (column.isStringType() && column.hasStrLength()) {
								// 字符类型并包含长度
								stringBuilder.append("(").append(column.getStrLength()).append(")");
							} else if (column.hasPrecision()) {
								// 数字类型包含精度和长度
								stringBuilder.append("(").append(column.getPrecision());
								if (column.hasScale()) {
									stringBuilder.append(",").append(column.getScale());
								}
								stringBuilder.append(")");
							}
						}
						column.setFinalConvertDataType(stringBuilder.toString());
					}
				} else {
					// 未配置映射关系
					if(column.getDataBaseType().equals(DataBaseType.ORACLE)) {
						column.setFinalConvertDataType("CLOB");
					}else {
						column.setFinalConvertDataType("TEXT");
					}
					logger.error(String.format("无法转换的的类型为：%s", column.getDataType()));
				}
				if(column.getFinalConvertDataType().contains("(")) {
					column.setDataType(column.getFinalConvertDataType().substring(0,column.getFinalConvertDataType().indexOf("(")));
				}else {
					column.setDataType(column.getFinalConvertDataType());
				}
			}
		}
		return dataBaseDefine;
	}

}
