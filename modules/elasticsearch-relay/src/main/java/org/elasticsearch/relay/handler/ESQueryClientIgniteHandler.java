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
 * Central query handler splitting up queries between multiple ES instances,
 * handling query filtering, sending requests, post-processing and merging
 * results.
 */
public class ESQueryClientIgniteHandler extends ESQueryKernelIgniteHandler{
	
	
	IgniteClient igniteClient;
	
	public ESQueryClientIgniteHandler(ESRelayConfig config) throws Exception{
		super(config,null);
		//thin
		String host = config.getElasticApiHost();		
		if(host!=null && !host.isEmpty()){
			
			ClientConfiguration cfg = new ClientConfiguration().setAddresses(host+":"+config.getElasticApiPort());	
	        try {
	        	IgniteClient igniteClient = Ignition.startClient(cfg);
	        			
	            System.out.println();
	            System.out.println(">>> Thin client ignite started.");          
	            this.igniteClient = igniteClient;
	        }
	        catch (ClientException e) {
	            System.err.println(e.getMessage());
	        }
	        catch (Exception e) {
	            System.err.format("Unexpected failure: %s\n", e);
	        }
	        
		}
		else{
	        //client node
			 Ignition.setClientMode(true);			
			 try{			 
				 ignite = Ignition.start("config/example-ignite.xml");
			 }
			 catch(Exception e){
				 fLogger.log(Level.SEVERE, e.getMessage(), e);
			 }
		}
	}
	
	/**
	 * Stops the permission crawler thread.
	 */
	public void destroy() {
		super.destroy();
		
		if(ignite!=null){
			ignite.close();
			ignite = null;
		}
		
		if(igniteClient!=null){
			try {
				igniteClient.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				fLogger.log(Level.SEVERE, e.getMessage(), e);
			}
		}
		
	}	

}
