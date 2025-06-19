package org.apache.ignite.console.agent.db;

import static org.apache.ignite.console.utils.Utils.fromJson;

import java.io.IOException;
import java.sql.Driver;
import java.sql.SQLException;
import javax.sql.DataSource;

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
import java.util.Hashtable;
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
import org.apache.commons.dbcp2.BasicDataSource;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class DataSourceManager {
	
	private static Map<String,Object> POOL = new ConcurrentHashMap<>();
	/** */
    public static final Map<String, Driver> drivers = new HashMap<>();
	private static InitialContext initialContext = null;
	private static HttpClient serverClient;
	private static String datasourceGetUrl = "";
	private static String datasourceCreateUrl = "";
	private static String taskflowGetUrl = "";
	private static Collection<String> serverTokens;
	
	static class DBinitialContext extends InitialContext {
		
		public DBinitialContext() throws NamingException {
			super(true);
		}
		
		public DBinitialContext(Hashtable<?,?> environment) throws NamingException {
			super(environment);
		}

	    public void bind(String key, Object value) {
	    	POOL.put(key.toLowerCase(), value);
	    }
	    
	    public void rebind(String key, Object value) {
	    	POOL.put(key.toLowerCase(), value);
	    }
	    
	    public void unbind(String name){
	    	POOL.remove(name);
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
	    
	    public void close() throws NamingException {
	    	
	    }
	};
	
	
	public static void init(HttpClient client,String serverUri,Collection<String> tokens) {
		serverClient = client;
		serverTokens = tokens;
		
		if(initialContext!=null) {
			return;
		}
		
		if (serverUri.startsWith("ws:/")) {
			serverUri = serverUri.replaceAll("ws:/", "http:/");
		}
		if (serverUri.startsWith("wss:/")) {
			serverUri = serverUri.replaceAll("ws:/", "https:/");
		}
		datasourceGetUrl = serverUri+"/api/v1/datasource";
		datasourceCreateUrl = serverUri+"/api/v1/datasource";
		taskflowGetUrl = serverUri+"/api/v1/taskflow/cluster/%s?target=%s";
		
		try {
			
			initialContext = new DBinitialContext();
			// Activate the initial context
			NamingManager.setInitialContextFactoryBuilder(environment -> environment1 -> new DBinitialContext());

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
		return bindDataSource(jndiName,info,drivers.get(info.getDriverCls()));
	}
	
	public static DataSource bindDataSource(String jndiName, DBInfo info, Driver driver) {

		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriver(driver);
		dataSource.setUrl(info.jdbcUrl);
		dataSource.setUsername(info.getUserName());
		dataSource.setPassword(info.getPassword());
		
		dataSource.setDefaultSchema(info.getSchemaName());
		
		
		if(info.jdbcUrl.startsWith("jdbc:h2:")) {			
			dataSource.setDriverClassName("org.h2.Driver");
		}
		else {
			dataSource.setDriverClassName(info.getDriverCls());
		}
		
		try {
			try {				
				initialContext.rebind("java:jdbc/"+jndiName, dataSource);				
			} catch (NamingException e) {
				initialContext.bind("java:jdbc/"+jndiName, dataSource);
			}
			
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
				req.header("Authorization", "token "+token);
				req.method(HttpMethod.GET);
				ContentResponse response = req.send();
				String body = response.getContentAsString();
				if(body.startsWith("[")) {
					TypeReference<List<DBInfo>> typeRef = new TypeReference<List<DBInfo>>() {};
					List<DBInfo> datasources  = fromJson(body,typeRef);
					for(DBInfo dbInfo: datasources) {
						if(dbInfo.getJndiName().equalsIgnoreCase(jndi)) {
							try {
								dataSource = bindDataSource(jndi, dbInfo);
								return dataSource;
							} catch (Exception e) {							
								e.printStackTrace();
							}
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
				req.header("Authorization", "token "+token);
				req.header("XSRF-TOKEN", token);
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
	
	public static JsonArray getTaskFlows(String clusterId,String cache) {
		JsonArray dataSource = null;
		String url = String.format(taskflowGetUrl,clusterId,cache);
		try {
			for(String token: serverTokens) {
				Request req = serverClient.newRequest(url);
				req.header("Authorization", "Token "+token);
				req.method(HttpMethod.GET);
				ContentResponse response = req.send();
				String body = response.getContentAsString();
				if(body.startsWith("[")) {					
					dataSource  = new JsonArray(body);
					break;
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
		return dataSource;
	}
}
