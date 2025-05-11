package org.apache.ignite.console.agent.service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.json.BinaryObjectUtil;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.resources.IgniteInstanceResource;
import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;

@ApiOperation("Run langflow for data to cache")
public class LangflowApiClient implements CacheAgentService{	
	
	private static final long serialVersionUID = 1L;
	

	/** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
    private String apiKey;
    
    private String baseUrl;
    
    public LangflowApiClient() {
    	// API Configuration
        apiKey = System.getenv("LANGFLOW_API_KEY");
        if (apiKey == null || apiKey.isEmpty()) {
            System.err.println("LANGFLOW_API_KEY environment variable not found. Please set your API key in the environment variables.");           
        }
       
        baseUrl = System.getenv("LANGFLOW_API_BASE");
        if (baseUrl == null || baseUrl.isEmpty()) {
        	baseUrl = "http://localhost:7860";        	
        }     
    }

	/**
	 * 
	 * @param endpoint
	 * @param input  "{\"input_value\":\"患有\\\"糖尿病\\\"的所有患者的就诊费用总和\",\"output_type\":\"chat\",\"input_type\":\"chat\"}";
	 * @return
	 */   
	public ServiceResult call(String endpoint,JsonObject payload) {
		ServiceResult result = new ServiceResult();
		
		if (apiKey == null || apiKey.isEmpty()) {           
            result.addMessage("LANGFLOW_API_KEY environment variable not found. Please set your API key in the environment variables.");
            result.setStatus("404");
            return result;
        }
		
        String url = baseUrl+"/api/v1/run/"+endpoint;        
        
        try {
            // Create connection
            URL apiUrl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("x-api-key", apiKey);
            connection.setDoOutput(true);
            connection.connect();
            // Send request
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = payload.encode().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }
            
            JsonObject output = null;
            
            // Get response
            int status = connection.getResponseCode();
            if (status >= 200 && status < 300) {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    System.out.println(response.toString());
                    
                    output = new JsonObject(response.toString());
                    result.result = output;
                }
                result.setStatus("200");
            } else {
                try (BufferedReader br = new BufferedReader(
                        new InputStreamReader(connection.getErrorStream(), StandardCharsets.UTF_8))) {
                    StringBuilder response = new StringBuilder();
                    String responseLine;
                    while ((responseLine = br.readLine()) != null) {
                        response.append(responseLine.trim());
                    }
                    System.err.println("API request failed with status code: " + status);
                    System.err.println(response.toString());                    
                    
                    result.setError(response.toString());
                }
                result.setStatus(""+status);
            }
            
            connection.disconnect();
            return result;
            
        } catch (Exception e) {
            System.err.println("Error making API request: " + e.getMessage());          
            result.setError(e.getMessage());
            result.setStatus("404");
            return result;
        }
	}

	@Override
	public ServiceResult call(Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();		
		JsonObject args = new JsonObject(payload);
		
        String endpoint = args.getString("endpoint");
        String inputField = args.getString("inputField");
        String outputCacheSuffix = args.getString("outputSuffix","_result");
        
        // Request payload configuration 
        int count = 0;
        List<String> message = result.getMessages();
        if(ignite!=null) {
			List<String> caches = cacheNameSelectList(ignite,args);
			for(String cache: caches) {
				try {
					IgniteCache<?,?> igcache = ignite.cache(cache).withKeepBinary();
					CacheConfiguration<Object,BinaryObject> cfg = igcache.getConfiguration(CacheConfiguration.class);
					CacheConfiguration<Object,BinaryObject> cfgOutput = new CacheConfiguration<Object,BinaryObject>(cfg);
					cfgOutput.setName(cache+""+outputCacheSuffix);
					IgniteCache<Object,BinaryObject> resultCache = ignite.getOrCreateCache(cfgOutput);
	
						
					Iterable<Cache.Entry<Object, Object>> it = (Iterable)igcache.localEntries(CachePeekMode.PRIMARY);
					for(Cache.Entry<Object, Object> row: it) {
						if(row instanceof BinaryObject) {
							BinaryObject node = (BinaryObject)row;
							JsonObject apiPayload = new JsonObject();
							Object input = node.field(inputField);
							if(input!=null) {
								apiPayload.put("input_value", input);
								ServiceResult rowResult = this.call(endpoint,apiPayload);
								BinaryObject output = BinaryObjectUtil.mapToBinaryObject(ignite, cfgOutput.getName(), rowResult.result.getMap());
								resultCache.put(row.getKey(), output);
							}
						}
						
						count++;
					}	
					
				}
				catch(Exception e) {
					message.add(e.getMessage());
					result.setError(e.getMessage());
				}
			}
        }
		result.put("caches", ignite.cacheNames());
		result.put("count", count);
        return result;
        
	}
}