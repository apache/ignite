package com.github.abel533;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

import org.apache.ignite.console.agent.db.BeetlTemplate;
import org.apache.ignite.console.agent.db.Dialect;
import org.apache.ignite.console.agent.db.IntrospectedTable;
import org.apache.ignite.console.agent.utils.DBMetadataUtils;
import org.postgresql.Driver;

/**
 * @author liuzh
 */
public class DatabasePostgreTest {

    public static void main(String[] args) throws Exception {
    	Properties prop = new Properties();
    	prop.put("user", "postgres");
    	prop.put("password", "332584185");
    	Driver driver = new Driver();
    	Connection conn = driver.connect("jdbc:postgresql://localhost/nlpteam", prop);
       
        DBMetadataUtils dbMetadataUtils = null;
        try {
            dbMetadataUtils = new DBMetadataUtils(conn,Dialect.POSTGRESQL);

            List<IntrospectedTable> tables = dbMetadataUtils.introspectTables(dbMetadataUtils.getDefaultConfig());

            DBMetadataUtils.sortTables(tables);

            BeetlTemplate.exportDatabaseHtml(tables, "/tmp", "db");

            for (IntrospectedTable table : tables) {
                BeetlTemplate.exportTableHtml(table, "/tmp", table.getName());
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
