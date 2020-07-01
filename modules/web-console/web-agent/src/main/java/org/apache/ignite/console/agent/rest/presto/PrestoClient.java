package org.apache.ignite.console.agent.rest.presto;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.util.StringUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

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
	/**
	 * The following parameters may be modified depending on your configuration
	 */
	private String $source = "PrestoClient";
	private String $version = "0.2";
	private int $maximumRetries = 5;
	private String $prestoUser = "presto";
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
	


	/**
	 * Constructs the presto connection instance
	 *
	 * @param $connectUrl
	 * @param $catalog
	 */
	public PrestoClient(OkHttpClient httpClient,String connectUrl,String catalog){
		this.$url = connectUrl;
		this.$prestoCatalog = catalog;
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
		
		this.$userAgent = this.$source+"/"+this.$version;
		
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
				
		this.$headers.put("X-Presto-User", this.$prestoUser);
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
	
	public void setUser(String user) {
		this.$prestoUser = user;
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
    
	public static void main(String[] args) {
		
		Dispatcher dispatcher = new Dispatcher();

        dispatcher.setMaxRequests(Integer.MAX_VALUE);
        dispatcher.setMaxRequestsPerHost(Integer.MAX_VALUE);

        OkHttpClient.Builder builder = new OkHttpClient.Builder()
            .readTimeout(0, TimeUnit.MILLISECONDS)
            .dispatcher(dispatcher);  
        
		//Create a new connection object. Provide URL and catalog as parameters
		PrestoClient  $presto = new PrestoClient(builder.build(),"http://localhost:9100","pgsql");

		//Prepare your sql request
		try {
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
