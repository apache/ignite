package database.ddl.transfer.factory.convert.impl;

import database.ddl.transfer.bean.Column;
import database.ddl.transfer.factory.convert.BaseTypeConverter;
import java.util.Map;



/**
 *@ClassName PostgreSql2MySQLTypeConverter
 *@Description TODO
 *@Author luoyuntian
 *@Date 2019-12-29 20:58
 *@Version
 **/
public class PostgreSql2MySQLTypeConverter extends BaseTypeConverter {
    public PostgreSql2MySQLTypeConverter(Map<String, String> typeMapping,Map<String, String> typeProperties) {
        super(typeMapping,typeProperties);
    }

    @Override
    public String convert(Column column) {
        return typeMapping.get(column.getColumnType().toUpperCase());
    }


}
