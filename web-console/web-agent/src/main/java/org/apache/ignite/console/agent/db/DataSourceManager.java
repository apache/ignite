package org.apache.ignite.console.agent.db;

import static org.apache.ignite.console.utils.Utils.fromJson;

import java.io.IOException;
import java.sql.SQLException;
import javax.sql.DataSource;

import org.apache.ignite.console.db.DBInfo;
import org.apache.ignite.console.json.JsonArray;
import org.apache.ignite.console.json.JsonObject;
import org.apache.ignite.console.utils.Utils;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.ContentResponse;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.util.StringContentProvider;
import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.websocket.client.WebSocketClient;
import org.springframework.jndi.JndiObjectFactoryBean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.naming.Context;
import javax.naming.InitialContext;

import javax.naming.NamingException;
import javax.naming.spi.NamingManager;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSourceManager {
	
	private static Map<String,Object> POOL = new ConcurrentHashMap<>();
	private static InitialContext initialContext;
	private static HttpClient serverClient;
	private static String datasourceGetUrl = "";
	private static String datasourceCreateUrl = "";
	private static Collection<String> serverTokens;
	
	
	public static void init(HttpClient client,String serverUri,Collection<String> tokens) {
		serverClient = client;
		serverTokens = tokens;
		if (serverUri.startsWith("ws:/")) {
			serverUri = serverUri.replaceAll("ws:/", "http:/");
		}
		if (serverUri.startsWith("wss:/")) {
			serverUri = serverUri.replaceAll("ws:/", "https:/");
		}
		datasourceGetUrl = serverUri+"/api/v1/datasource";
		datasourceCreateUrl = serverUri+"/api/v1/datasource";
		
		try {
			initialContext = new InitialContext() {		    

			    public void bind(String key, Object value) {
			    	POOL.put(key.toLowerCase(), value);
			    }

			    public Object lookup(String key) throws NamingException {
			        Object result = POOL.get(key.toLowerCase());
			        if(result==null && key.startsWith("java:jdbc/")) {
			        	result = getJNDIDataSource(key.substring("java:jdbc/".length()));
			        	if(result!=null) {
			        		POOL.put(key, result);
			        	}
			        }
			        return result;
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
	
	public static DataSource getDataSource(String jndiName) throws SQLException {
		DataSource dataSource = null;
		try {
			if(jndiName.startsWith("java:jdbc/")) {
				dataSource = (DataSource) initialContext.lookup(jndiName);
			}
			else {
				dataSource = (DataSource) initialContext.lookup("java:jdbc/"+jndiName);
			}
			
		} catch (NamingException e) {
			
		}
		if(dataSource==null) {
		    throw new java.sql.SQLException("Datasource is not binded! jndiName:"+jndiName);
		}	
		return dataSource;
	}
	
	
	public static DataSource bindDataSource(String jndiName, DBInfo info) {
		HikariConfig config = new HikariConfig();
		config.setMinimumIdle(1);
		config.setMaximumPoolSize(8);
		config.setJdbcUrl(info.jdbcUrl);
		config.setUsername(info.getUserName());
	    config.setPassword(info.getPassword());
	    
		if(info.getJdbcProp()!=null) {
		    config.setDataSourceProperties(info.getJdbcProp());
		}

		DataSource dataSource = new HikariDataSource(config);
		
		if(info.jdbcUrl.startsWith("jdbc:h2:")) {
			config.setIsolateInternalQueries(true);
			config.setDriverClassName("org.h2.Driver");			
			dataSource = new HikariDataSource(config);
		}
		else {
			config.setDriverClassName(info.getDriverCls());
			dataSource = new HikariDataSource(config);
		}
		
		try {
			initialContext.bind("java:jdbc/"+jndiName, dataSource);
		} catch (NamingException e) {
			e.printStackTrace();
		}
		return dataSource;

	}
	
	
	private static DataSource getJNDIDataSource(String jndi) {
		DataSource dataSource = null;
		try {
			for(String token: serverTokens) {
				Request req = serverClient.newRequest(datasourceGetUrl);
				req.header("TOKEN", token);
				req.method(HttpMethod.GET);
				ContentResponse response = req.send();
				String body = response.getContentAsString();
				TypeReference<List<DBInfo>> typeRef = new TypeReference<List<DBInfo>>() {};
				List<DBInfo> datasources  = fromJson(body,typeRef);
				for(DBInfo dbInfo: datasources) {
					if(dbInfo.getJndiName().equalsIgnoreCase(jndi)) {
						try {
							dataSource = bindDataSource(jndi, dbInfo);
							return dataSource;
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}				
			}
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ExecutionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TimeoutException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		return dataSource;
	}
	

	public static JsonObject createDataSource(String id, DBInfo dbInfo) {
		
		try {	
			for(String token: serverTokens) {
				Request req = serverClient.newRequest(datasourceCreateUrl);
				req.header("TOKEN", token);
				req.header("XSRF-TOKEN", "a9bd0b6d-eb01-4656-945e-10fa264e15a1");
				req.method(HttpMethod.PUT);
				String content = Utils.toJson(dbInfo);
				req.content(new StringContentProvider(content, "UTF-8"),"application/json");
				ContentResponse response = req.send();
				String body = response.getContentAsString();
				
				JsonObject result = fromJson(body);
				if(result.getString("error")==null) {
					return result;
				}
			}
			
			
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ExecutionException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (TimeoutException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return null;
	}
	

}
