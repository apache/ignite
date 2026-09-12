package org.apache.ignite.console.agent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.ignite.console.agent.db.dialect.PostgreSQLMetadataDialect;
import org.junit.Test;
import org.postgresql.Driver;
import org.springframework.jndi.JndiObjectFactoryBean;

public class DataSourceManagerTest {
	
	@Test
	public void testMetaData() throws SQLException {
		Properties prop = new Properties();
    	prop.put("user", "postgres");
    	prop.put("password", "332584185");
    	Driver driver = new Driver();
    	Connection conn = driver.connect("jdbc:postgresql://localhost/nlpteam", prop);
		
		PostgreSQLMetadataDialect dialect = new PostgreSQLMetadataDialect(conn);
		
		var list = dialect.tables(conn,new ArrayList<>(), false);
		System.out.println(list);
	}
	
	private DataSource testCreatedsHiaHosBase()  {    	
        
        JndiObjectFactoryBean factoryBean = new JndiObjectFactoryBean();
        factoryBean.setJndiName("dsPostgreSQL_HiaHosBase");
        //factoryBean.setProxyInterface(DataSource.class);
        //factoryBean.afterPropertiesSet();
        //DataSource dataSource = (DataSource)factoryBean.getObject();
        
        DataSource dataSource;
		try {
			
			dataSource = factoryBean.getJndiTemplate().lookup("java:jdbc/dsPostgreSQL_HiaHosBase", DataSource.class);
			return dataSource;
		
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
		return null;
    }

}
