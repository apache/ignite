package org.apache.ignite.console.agent.db;

import com.fasterxml.jackson.core.type.TypeReference;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.ignite.console.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.naming.spi.NamingManager;
import javax.sql.DataSource;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.ignite.console.utils.Utils.fromJson;

public class DataSourceManager {
	private static final Logger log = LoggerFactory.getLogger(DataSourceManager.class);
	/** */
    public static final Map<String, Driver> drivers = new HashMap<>();
	private static final Map<String,DBinitialContext> initialContextMap = new ConcurrentHashMap<>();
	private static HttpClient httpClient;
	private static String datasourceGetUrl = "";
	private static String datasourceCreateUrl = "";
	private static String taskflowGetUrl = "";
	private static final ThreadLocal<String> accountToken = new ThreadLocal<>();

	private static Collection<String> validTokens;

	public static String getAccountToken() {
		return DataSourceManager.accountToken.get();
	}

	public static void setAccountToken(String accountToken) {
		DataSourceManager.accountToken.set(accountToken);
	}


	static class DBinitialContext extends InitialContext {
		private final String _accountToken;
		private final Map<String,Object> POOL = new ConcurrentHashMap<>();
		
		public DBinitialContext(String accountToken) throws NamingException {
			super(true);
			this._accountToken = accountToken;
		}
		
		public DBinitialContext(Hashtable<?,?> environment,String accountToken) throws NamingException {
			super(environment);
			this._accountToken = accountToken;
		}

	    @Override
		public void bind(String key, Object value) throws NamingException{
	    	POOL.put(key.toLowerCase(), value);
	    }
	    
	    @Override
		public void rebind(String key, Object value) throws NamingException{
	    	POOL.put(key.toLowerCase(), value);
	    }
	    
	    @Override
		public void unbind(String name) {
	    	POOL.remove(name);
	    }

	    @Override
		public Object lookup(String key) throws NamingException {
	        Object result = POOL.get(key.toLowerCase());
	        if(result==null && key.startsWith("java:jdbc/")) {
	        	result = getJNDIDataSource(key.substring("java:jdbc/".length()),_accountToken);
	        	if(result!=null) {
	        		POOL.put(key, result);
	        	}
	        }
	        return result;
	    }
	    
	    @Override
		public void close() throws NamingException {
			POOL.clear();
	    }
	};

	public static DBinitialContext getDBinitialContext(String accountToken) throws NamingException{
		DBinitialContext ctx = initialContextMap.get(accountToken);
		if(ctx==null) {
			ctx = new DBinitialContext(accountToken);
			initialContextMap.put(accountToken,ctx);
		}
		return ctx;
	}

	public static DBinitialContext getDBinitialContext(Hashtable<?,?> environment,String accountToken) throws NamingException{
		DBinitialContext ctx = initialContextMap.get(accountToken);
		if(ctx==null) {
			ctx = new DBinitialContext(environment,accountToken);
			initialContextMap.put(accountToken,ctx);
		}
		return ctx;
	}
	
	public DataSourceManager(String serverUri, Collection<String> tokens) {
		if (serverUri.startsWith("ws:/")) {
			serverUri = serverUri.replaceAll("ws:/", "http:/");
		}
		if (serverUri.startsWith("wss:/")) {
			serverUri = serverUri.replaceAll("ws:/", "https:/");
		}
		validTokens = tokens;
		datasourceGetUrl = serverUri+"/api/v1/datasource";
		datasourceCreateUrl = serverUri+"/api/v1/datasource";
		taskflowGetUrl = serverUri+"/api/v1/taskflow/cluster/%s?target=%s";

		if(httpClient!=null) {
			return;
		}

		this.httpClient = HttpClient.newBuilder()
				.connectTimeout(Duration.ofMillis(60000))
				.version(HttpClient.Version.HTTP_1_1)
				.build();

		try {
			// Activate the initial context
			NamingManager.setInitialContextFactoryBuilder(environment ->
				environment2 -> {
					return getDBinitialContext(environment2,accountToken.get());
				}
			);

		} catch (NamingException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}        
	}

	public static DataSource getDataSource(String jndiName) throws SQLException {
		return getDataSource(jndiName,accountToken.get());
	}
	
	public static DataSource getDataSource(String jndiName,String accountToken) throws SQLException {
		DataSource dataSource = null;
		try {
			DBinitialContext ctx = getDBinitialContext(accountToken);
			if(jndiName.startsWith("java:jdbc/")) {
				dataSource = (DataSource) ctx.lookup(jndiName);
			}
			else {
				dataSource = (DataSource) ctx.lookup("java:jdbc/"+jndiName);
			}
			
		} catch (NamingException e) {
			log.error("Failed to get datasource entry: "+jndiName, e);
		}
		if(dataSource==null) {
		    throw new SQLException("Datasource is not binded! jndiName:"+jndiName);
		}	
		return dataSource;
	}
	
	public static DataSource bindDataSource(String jndiName, DbInfo info,String accountToken) {
		return bindDataSource(jndiName,info,drivers.get(info.getDriverCls()),accountToken);
	}

	public static DataSource bindDataSource(String jndiName, DbInfo info, Driver driver,String accountToken) {

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
			DBinitialContext ctx = getDBinitialContext(accountToken);
			ctx.rebind("java:jdbc/"+jndiName, dataSource);

		} catch (NamingException e) {
			log.error("Failed to bind datasource entry: "+jndiName, e);
		}
		return dataSource;

	}

	private static DataSource getJNDIDataSource(String jndi, String accountToken) {
		DataSource dataSource = null;
		HttpRequest request = HttpRequest.newBuilder().version(HttpClient.Version.HTTP_1_1)
				.uri(URI.create(datasourceGetUrl))
				.header("Authorization", "token " + accountToken)
				.GET()
				.build();
		try {
			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			String body = response.body();
			if(body.startsWith("[")) {
				TypeReference<List<DbInfo>> typeRef = new TypeReference<List<DbInfo>>() {};
				List<DbInfo> datasources  = fromJson(body,typeRef);
				for(DbInfo dbInfo: datasources) {
					DataSource dataSource2 = bindDataSource(jndi, dbInfo,accountToken);
					if(dbInfo.getJndiName().equalsIgnoreCase(jndi)) {
						dataSource = dataSource2;
					}
				}
			}
		} catch (InterruptedException | IOException e) {
			log.error("Failed to get dbinfo entry: " + datasourceGetUrl, e);
		}
        return dataSource;
	}

	public static String createDataSource(String id, DbInfo dbInfo,String accountToken) {
		String content = Utils.toJson(dbInfo);
		HttpRequest request = HttpRequest.newBuilder().version(HttpClient.Version.HTTP_1_1)
				.uri(URI.create(datasourceCreateUrl))
				.header("Authorization", "token " + accountToken)
				.header("ContentType","application/json;UTF-8")
				.PUT(HttpRequest.BodyPublishers.ofString(content))
				.build();
		try {
			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			String body = response.body();
			return body;
			
		} catch (InterruptedException | IOException e1) {
			log.error("Failed to save dbinfo entry: " + datasourceGetUrl, e1);
		}
        return null;
	}

	public static JsonArray getTaskFlows(String clusterId,String cache) {
		for(String accountToken: validTokens){
			JsonArray list = getTaskFlows(clusterId,cache,accountToken);
			if(list!=null){
				return list;
			}
		}
		return null;
	}
	
	public static JsonArray getTaskFlows(String clusterId,String cache,String accountToken) {
		String url = String.format(taskflowGetUrl,clusterId,cache);
		HttpRequest request = HttpRequest.newBuilder().version(HttpClient.Version.HTTP_1_1)
				.uri(URI.create(url))
				.header("Authorization", "token " + accountToken)
				.GET()
				.build();
		JsonArray taskFlows = null;
		try {
			HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
			String body = response.body();
			if(body.startsWith("[")) {
				taskFlows  = new JsonArray(body);
			}
		} catch (InterruptedException | IOException e) {
			log.error("Failed to get task flows: " + url, e);
		}
        return taskFlows;
	}
}
