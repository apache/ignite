package org.apache.ignite.console.agent.rest.presto;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.util.StringUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Maps;

import okhttp3.Dispatcher;
import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

public class PrestoClient {
	
	 /** JSON object mapper. */
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final JsonNodeFactory jsonNodeFactory = new JsonNodeFactory(true);
	/**
	 * The following parameters may be modified depending on your configuration
	 */
	
	private String $version = "0.2";
	private int $maximumRetries = 5;
	
	private String $prestoSchema = "default";
	private String $prestoCatalog = "hive";
	private String $userAgent = "";
	
	//Do not modify below this line
	private String $nextUri =" ";
	private String $infoUri = "";
	private String $partialCancelUri = "";
	private String $state = "NONE";
	
	private String $url;
	public Map<String,Object> $headers = new HashMap<>();
	private String $result;
	
	
	public String $queryId;
	public String $HTTP_error;
	public ArrayNode $data = null;	
	public ArrayNode $columns = null;
	
	 /** */
    private final OkHttpClient httpClient;
	
    private final ClientSession $session;

	/**
	 * Constructs the presto connection instance
	 *
	 * @param $connectUrl
	 * @param $catalog
	 */
	PrestoClient(OkHttpClient httpClient,ClientSession session){
		this.$session = session;
		this.$url = session.getServer().toString();
		this.$prestoCatalog = session.getCatalog();
		this.$prestoSchema = session.getSchema();
		this.httpClient = httpClient;
	}
	
	public void setSchema(String name) {
		this.$prestoSchema = name;
	}
	
	/**
	 * Return Data as an array. Check that the current status is FINISHED
	 *
	 * @return array|false
	 */
	public ArrayNode GetData(){
		if (!this.$state.equals("FINISHED")){
			return null;
		}
		return this.$data;
	}
	
	/**
	 * Return Data as an array. Check that the current status is FINISHED
	 *
	 * @return array|false
	 */
	public ArrayNode GetColumns(){
		if (!this.$state.equals("FINISHED")){
			return null;
		}
		return this.$columns;
	}

	/**
	 * prepares the query
	 *
	 * @param $query
	 * @return bool
	 * @throws IOException 
	 * @throws Exception
	 */
	public boolean Query(String $query) throws IOException {
		
		$data = null;
		
		this.$userAgent = $session.getSource()+"/"+this.$version;
		
		String $request = $query;
		//check that no other queries are already running for this object
		if (this.$state.equals("RUNNING")) {
			return false;
		}

		/**
		 * check that query is completed, and that we don't start
		 * a new query before the previous is finished
		 */
		if ($query.isEmpty()) {
			return false;
		}
				
		this.$headers.put("X-Presto-User", $session.getUser());
		this.$headers.put("X-Presto-Catalog", this.$prestoCatalog);
		if(!StringUtil.isEmpty(this.$prestoSchema)) {
			this.$headers.put("X-Presto-Schema", this.$prestoSchema);
		}
		this.$headers.put("User-Agent", this.$userAgent);
		this.$headers.put("Content-Type", "application/json; charset=utf-8");
		
		RequestBody body = RequestBody.create(MediaType.get("text/plain; charset=utf-8"), $request);
		Response resp = sendRequest(this.$url+"/v1/statement",body,this.$headers);
			
		
		int $httpCode = resp.code();
	
		if($httpCode!=200){
			
			this.$HTTP_error = resp.message();
			throw new IOException("HTTP ERROR: "+this.$HTTP_error);
		}
		
		this.$result = resp.body().string();
		
		//set status to RUNNING
		resp.close();
		this.$state = "RUNNING";
		return true;	
	}


	/**
	 * waits until query was executed
	 *
	 * @return bool
	 * @throws IOException 
	 * @throws PrestoException
	 */
	public boolean WaitQueryExec() throws IOException {
		
		this.GetVarFromResult();
		
		while (this.$nextUri!=null){
			
			try {
				Thread.sleep(1000);
				this.$result = file_get_contents(this.$nextUri,this.$headers);
				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				System.err.println(GetInfo());
				break;
			}
			
			this.GetVarFromResult();
		}
		
		if (!this.$state.equals("FINISHED")){
			throw new IOException("Incoherent State at end of query");}
		
		return true;
		
	}
	
	
	
	/** 
	 * Provide Information on the query execution
	 * The server keeps the information for 15minutes
	 * Return the raw JSON message for now
	 *
	 * @return string
	 */
	private String GetInfo() {
		
		String $infoRequest = "";
		try {
			$infoRequest = file_get_contents(this.$infoUri,this.$headers);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return $infoRequest;
	}
	
	private void GetVarFromResult() {
		/* Retrieve the variables from the JSON answer */
		
		try {
			ObjectNode  $decodedJson = (ObjectNode)MAPPER.readTree(this.$result);			

			if ($decodedJson.has("id")){
		  		this.$queryId = $decodedJson.get("id").asText();
		  	}
			
		  	if ($decodedJson.has("nextUri")){
		  		this.$nextUri = $decodedJson.get("nextUri").asText();
		  	}
		  	else {
		  		this.$nextUri = null;
		  	}
		  	
		  	if ($decodedJson.has("columns")){
		  		ArrayNode arr = $decodedJson.withArray("columns");
		  		this.$columns = arr;
		  	}
		  
		  	if ($decodedJson.has("data")){
		  		ArrayNode arr = $decodedJson.withArray("data");
		  		if(this.$data==null) {
		  			this.$data = arr;
		  		}
		  		else {
		  			this.$data.addAll(arr);
		  		}
		  	}
		  	
		  
		  	if ($decodedJson.has("infoUri")){
		  		this.$infoUri = $decodedJson.get("infoUri").asText();
		  	}
		  
		  	if ($decodedJson.has("partialCancelUri")){
		  		this.$partialCancelUri = $decodedJson.get("partialCancelUri").asText();
		  	}
		  
		  	if ($decodedJson.has("stats")){
		  		this.$state = $decodedJson.get("stats").get("state").asText();
		  	}	
		  	
		} catch (JsonMappingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	    	
	  
	}
	
	public void Close() {
		if (httpClient != null) {
            httpClient.dispatcher().executorService().shutdown();

            httpClient.dispatcher().cancelAll();
        }     
	}
	/**
	 * Provide a function to cancel current request if not yet finished
	 */
	public boolean Cancel(){
		if (StringUtil.isEmpty(this.$partialCancelUri)){
			return false; 
		}
			
		Response $infoRequest = null;
		try {
			$infoRequest = sendRequest(this.$partialCancelUri,null,this.$headers);
			
			if($infoRequest.code()!=204){
				return false;
			}else{
				return true;
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false; 
		}
		finally {
			if($infoRequest!=null)
				$infoRequest.close();
		}
	}
	

	/**
	 * 
	 * @param catalog,schema
	 * @return Map<String,JSONNode>
	 */
	public ObjectNode ShowTables(String catalog,String schema){
		if(StringUtil.isEmpty(catalog)) {
			catalog = $session.getCatalog();
		}
		Map<String,ObjectNode> schemas = ShowSchemas(catalog);
		if(schemas.containsKey(schema)) {
			ObjectNode tables = schemas.get(schema);
			if(tables.isEmpty()) {
				try {
					this.Query("SHOW TABLES FROM "+catalog+'.'+schema);
					
					if(this.WaitQueryExec()) {
						ArrayNode catas = this.GetData();
						if(catas!=null)
							catas.forEach(item->{
								ObjectNode table  = new ObjectNode(jsonNodeFactory);
								String name = item.get(0).asText();
								tables.set(name, table);
							});
						
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			return tables;
		}
		else {
			ObjectNode tables  = new ObjectNode(jsonNodeFactory);
			return tables;					
		}
	}
	
	/**
	 * 
	 * @param catalog
	 * @return Map<scheam:catalog>
	 */
	public Map<String,ObjectNode> ShowSchemas(String catalog){
		if(StringUtil.isEmpty(catalog)) {
			catalog = $session.getCatalog();
		}
		Set<String> catalogs = ShowCatalog();
		if(catalogs.contains(catalog)) {
			Map<String,ObjectNode> schemas = $session.metasMap.get(catalog);
			if(schemas.isEmpty()) {
				try {
					this.Query("SHOW SCHEMAS FROM "+catalog);
					
					if(this.WaitQueryExec()) {
						ArrayNode catas = this.GetData();
						catas.forEach(item->{
							ObjectNode tables  = new ObjectNode(jsonNodeFactory);
							String name = item.get(0).asText();
							schemas.put(name, tables);
						});
						
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
			return schemas;
		}
		else {
			return Maps.newHashMap();					
		}
	}
	
	/**
	 * 
	 * @param 
	 * @return <catalog type>
	 */
	public Set<String> ShowCatalog(){
		if($session.metasMap.isEmpty()) {
			try {
				this.Query("SHOW CATALOGS");
				if(this.WaitQueryExec()) {
					ArrayNode catas = this.GetData();
					catas.forEach(item->{
						String name = item.get(0).asText();
						Map<String,ObjectNode> schema = new HashMap<>();
						$session.metasMap.put(name, schema);
					});
					
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		return $session.metasMap.keySet();
	}

    /** */
    public Response sendRequest(String url, RequestBody body, Map<String, Object> headers) throws IOException {
        HttpUrl httpUrl = HttpUrl
            .parse(url)
            .newBuilder()          
            .build();

        final Request.Builder reqBuilder = new Request.Builder();

        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet())
                if (entry.getValue() != null)
                    reqBuilder.addHeader(entry.getKey(), entry.getValue().toString());
        }       
        if(body!=null) {
        	reqBuilder.url(httpUrl).post(body);
        }
        else {
        	reqBuilder.url(httpUrl).get();
        }

        Response resp = httpClient.newCall(reqBuilder.build()).execute();
        return resp;
        
    }

    /** */
    public String file_get_contents(String url, Map<String, Object> headers) throws IOException {
        HttpUrl httpUrl = HttpUrl
            .parse(url)
            .newBuilder()          
            .build();

        final Request.Builder reqBuilder = new Request.Builder();

        if (headers != null) {
            for (Map.Entry<String, Object> entry : headers.entrySet())
                if (entry.getValue() != null)
                    reqBuilder.addHeader(entry.getKey(), entry.getValue().toString());
        }       

        reqBuilder.url(httpUrl).get();

        try (Response resp = httpClient.newCall(reqBuilder.build()).execute()) {
            return resp.body().string();
        }
    }
    
	public static void main(String[] args) throws URISyntaxException {
		
		ClientSession session = new ClientSession(new URI("http://localhost:9100"),"root","demo","pgsql",null);
        
		//Create a new connection object. Provide URL and catalog as parameters
		PrestoClient  $presto = session.newPrestoClient();

		//Prepare your sql request
		try {
			
			ObjectNode tables = $presto.ShowTables(null, "public");
			
			$presto.Query("SELECT * FROM pgsql.dict.mid_city_area");
			
			//Execute the request and build the result
			$presto.WaitQueryExec();

			//Get the result
			ArrayNode $answer = $presto.GetData();

			System.out.println($answer);
			
		} catch (IOException $e) {
			$e.printStackTrace();
		}

		
	}
}
