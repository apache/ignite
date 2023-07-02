package org.elasticsearch.relay.handler;

import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import javax.cache.Cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.TextQuery;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;
import org.elasticsearch.relay.ESRelayConfig;
import org.elasticsearch.relay.model.ESQuery;
import org.elasticsearch.relay.model.ESResponse;
import org.elasticsearch.relay.model.ESUpdate;
import org.elasticsearch.relay.postprocess.IPostProcessor;
import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.HttpUtil;


/**
 * Central query handler splitting up queries between multiple ignite instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryClientIgniteHandler extends ESQueryKernelIgniteHandler{
	
	
	IgniteClient[] igniteClient;
	
	public ESQueryClientIgniteHandler(ESRelayConfig config) throws Exception{
		super(config,null);		
		
		igniteClient = new IgniteClient[2];
		
		//thin
		String host = config.getElasticApiHost();	
		int port =  config.getElasticApiPort();
		if(host!=null && !host.isEmpty()){
			
			ClientConfiguration cfg = new ClientConfiguration().setAddresses(host+":"+port);	
	        try {
	        	IgniteClient igniteClient = Ignition.startClient(cfg);
	        			
	            System.out.println();
	            System.out.println(">>> Thin client ignite started.");          
	            this.igniteClient[0] = igniteClient;
	        }
	        catch (ClientException e) {
	            System.err.println(e.getMessage());
	            fLogger.log(Level.SEVERE, e.getMessage(), e);
	        }
	        catch (Exception e) {
	            System.err.format("Unexpected failure: %s\n", e);
	            fLogger.log(Level.SEVERE, e.getMessage(), e);
	        }
	        
		}
		
		host = config.getEs2ElasticApiHost();	
		port = config.getEs2ElasticApiPort();
		if(host!=null && !host.isEmpty()){
			
			ClientConfiguration cfg = new ClientConfiguration().setAddresses(host+":"+port);	
	        try {
	        	IgniteClient igniteClient = Ignition.startClient(cfg);
	        			
	            System.out.println();
	            System.out.println(">>> Thin client ignite started.");          
	            this.igniteClient[1] = igniteClient;
	        }
	        catch (ClientException e) {
	            System.err.println(e.getMessage());
	            fLogger.log(Level.SEVERE, e.getMessage(), e);
	        }
	        catch (Exception e) {
	            System.err.format("Unexpected failure: %s\n", e);
	            fLogger.log(Level.SEVERE, e.getMessage(), e);
	        }
	        
		}
		
	}
	
	/**
	 * Stops the permission crawler thread.
	 */
	public void destroy() {
		super.destroy();
		
		for(IgniteClient client: igniteClient){
			try {
				if(client!=null) {
					client.close();
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				fLogger.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		
	}	

}
