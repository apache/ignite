package de.kp.works.ignite;


import org.apache.ignite.binary.*;

import java.util.Optional;

import org.apache.ignite.*;

public class IgniteConnect
{
	public static Ignite defaultIgnite = null;
	private final Ignite ignite;
    private final String graphNS;
    
    
    public static IgniteConnect getInstance(final String namespace) {
        return new IgniteConnect(namespace);
    }
    
    public String graphNS() {
        return this.graphNS;
    }
    
    private Ignite ignite() {
        return ignite;
    }
  
    
    public Ignite getIgnite() {
        return this.ignite();
    }
    
    
    public IgniteCache<String, BinaryObject> getOrCreateCache(final String name) {
        final IgniteCache<String, BinaryObject> cache = IgniteUtil.MODULE$.getOrCreateCache(this.ignite(), name, this.graphNS());
        if (cache == null) {
            throw new RuntimeException("Connection to Ignited failed. Could not create cache.");
        }
        return cache;
    }
    
    public boolean cacheExists(final String cacheName) {
        return this.ignite().cacheNames().contains(cacheName);
    }
    
    public void deleteCache(final String cacheName) {
        final boolean exists = this.ignite().cacheNames().contains(cacheName);
        if (exists) {
            this.ignite().cache(cacheName).destroy();
        }
    }
    
    public Ignite getOrStart(String instanceName) {
    	try {
    		if(defaultIgnite==null) {
    			if(Ignition.allGrids().size()==0) {
    				defaultIgnite = Ignition.start(IgniteConf.fromFile());
    			}
    			else {
    				defaultIgnite = Ignition.allGrids().get(0);
    			}
    		}
    		Ignite ignite = Ignition.ignite(instanceName);
    		return ignite;
    	}
    	catch(Exception e) {
    		return defaultIgnite;
    	}
        
    }
    
    public IgniteConnect(final String graphNS) {
        this.graphNS = graphNS;       
        this.ignite = this.getOrStart(graphNS);               
    }
}
