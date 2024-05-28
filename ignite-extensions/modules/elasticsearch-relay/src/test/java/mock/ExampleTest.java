package mock;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentHelper;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;


public class ExampleTest {

    private RestHighLevelClient hClient;
    private RestClient lClient;

    @Before
    public void setUp() {
    	String url = "http://127.0.0.1:18080/es-relay";
    	var b = RestClient.builder(new HttpHost("127.0.0.1", 18080));
    	b.setPathPrefix("es-relay");
        hClient = new RestHighLevelClient(b);
        lClient = hClient.getLowLevelClient();
    }

    @After
    public void clear(){
       
    }
    
    @Test
    public void testSearch() throws IOException {
    	
    	Request request = new Request(
    		    "GET",  
    		    "/test");   
    	Response response = lClient.performRequest(request);
    	
    	System.out.println(response.getEntity());
    }
    
    @Test
    public void testMGet() throws IOException {
    	
    	Request request = new Request(
    		    "GET",  
    		    "/test/_all");   
    	Response response = lClient.performRequest(request);
    	
    	MultiGetRequest multiGetRequest = new MultiGetRequest();
    	multiGetRequest.add("index","1");
    	multiGetRequest.add("index","2");
    	
    	MultiGetResponse resp = hClient.mget(multiGetRequest, RequestOptions.DEFAULT);
    	
    	System.out.println(resp.getResponses());
    }

    
}