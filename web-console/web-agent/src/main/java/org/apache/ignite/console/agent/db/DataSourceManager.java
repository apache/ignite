package org.apache.ignite.console.agent.db;

import java.sql.SQLException;
import javax.sql.DataSource;

import org.springframework.jndi.JndiObjectFactoryBean;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.naming.NamingException;
import javax.naming.spi.NamingManager;


import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSourceManager {
	private static Map<String,Object> POOL = new ConcurrentHashMap<>();
	private static InitialContext initialContext;
	
	{
		init();
	}
	
	private static void init() {
		
		try {
			initialContext = new InitialContext() {		    

			    public void bind(String key, Object value) {
			    	POOL.put(key.toLowerCase(), value);
			    }

			    public Object lookup(String key) throws NamingException {
			        return POOL.get(key.toLowerCase());
			    }
			};
			
			// Activate the initial context
			NamingManager.setInitialContextFactoryBuilder(environment -> environment1 -> initialContext);
			
		} catch (NamingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}        
	}
	
	public DataSource getDataSource(String url) throws SQLException {
		DataSource dataSource = null;
		try {
			dataSource = (DataSource) initialContext.lookup(url);
		} catch (NamingException e) {
			
		}
		if(dataSource==null) {
		    throw new java.sql.SQLException("Datasource is not binded! jdbcUrl:"+url);
		}	
		return dataSource;
	}
	
	
	public DataSource bindDataSource(String jndiName, String jdbcUrl,Properties info) throws SQLException {
		HikariConfig config = new HikariConfig();
		config.setMinimumIdle(1);
		config.setMaximumPoolSize(8);
		config.setJdbcUrl(jdbcUrl);
		if(info!=null) {
		    //config.setUsername(user);
		    //config.setPassword(password);
		    
		    config.setDataSourceProperties(info);
		}

		DataSource dataSource = new HikariDataSource(config);
		
		if(jdbcUrl.startsWith("jdbc:h2:")) {
			config.setIsolateInternalQueries(true);
			config.setDriverClassName("org.h2.Driver");			
			dataSource = new HikariDataSource(config);
		}
		else {
			dataSource = new HikariDataSource(config);
		}
		
		POOL.put(jdbcUrl, dataSource);
		
		try {
			initialContext.bind(jndiName, dataSource);
		} catch (NamingException e) {
			throw new SQLException(e);
		}
		return dataSource;

	}
	
	private static void test(String [] args) throws NamingException, SQLException {
		DataSourceManager dsMng = new DataSourceManager();
		
		Properties config = new Properties();
		config.put("user", "junphine");
		config.put("password", "332584185");
		
		DataSource sys = dsMng.bindDataSource("dsPostgreSQL_HiaHosBase","jdbc:postgresql://127.0.0.1:5432/drupal_his_db",config);
		DataSource sys2=dsMng.getDataSource("dsPostgreSQL_HiaHosBase");
		
		Context initContext = new InitialContext();
		initContext.bind("java:jdbc/dsPostgreSQL_HiaHosBase", sys);
		
		DataSource sys3 = createdsHiaHosBase();

	}
	
	private static DataSource createdsHiaHosBase()  {    	
        
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
