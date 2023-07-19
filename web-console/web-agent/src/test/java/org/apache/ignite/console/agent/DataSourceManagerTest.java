package org.apache.ignite.console.agent;

import javax.naming.NamingException;
import javax.sql.DataSource;

import org.springframework.jndi.JndiObjectFactoryBean;

public class DataSourceManagerTest {
	
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
