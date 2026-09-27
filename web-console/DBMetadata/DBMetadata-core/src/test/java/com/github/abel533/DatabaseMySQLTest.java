package com.github.abel533;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.ignite.console.agent.db.Dialect;
import org.apache.ignite.console.agent.db.IntrospectedColumn;
import org.apache.ignite.console.agent.db.IntrospectedTable;
import org.apache.ignite.console.agent.utils.DBMetadataUtils;

/**
 * @author liuzh
 */
public class DatabaseMySQLTest {

    public static void main(String[] args) throws SQLException {
    	Properties prop = new Properties();
    	prop.put("user", "root");
    	prop.put("password", "123456");
    	com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
    	Connection conn = driver.connect("jdbc:mysql://localhost:3306/test", prop);
       
        DBMetadataUtils dbMetadataUtils = null;
        try {
            dbMetadataUtils = new DBMetadataUtils(conn,Dialect.MYSQL);

            List<IntrospectedTable> list = dbMetadataUtils.introspectTables(dbMetadataUtils.getDefaultConfig());

            for (IntrospectedTable table : list) {
            	System.out.println(table.getName() + ":" + table.getRemarks());
                for (IntrospectedColumn column : table.getAllColumns()) {
                    System.out.println(column.getName() + " - " +
                            column.getJdbcTypeName() + " - " +
                            column.getJavaProperty() + " - " +
                            column.getJavaProperty() + " - " +
                            column.getFullyQualifiedJavaType().getFullyQualifiedName() + " - " +
                            column.getRemarks());
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
