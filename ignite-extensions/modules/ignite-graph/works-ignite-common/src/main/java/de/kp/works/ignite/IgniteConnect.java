package de.kp.works.ignite;


import org.apache.ignite.binary.*;

import java.util.Optional;

import org.apache.ignite.*;

public class IgniteConnect
{
	private static Ignite defaultIgnite = null;
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
    		Ignite ignite = Ignition.ignite(instanceName);
    		return ignite;
    	}
    	catch(Exception e) {
    		if(defaultIgnite==null) {
    			defaultIgnite= Ignition.getOrStart(IgniteConf.fromConfig());
    		}
    		return defaultIgnite;
    	}
        
    }
    
    public IgniteConnect(final String graphNS) {
        this.graphNS = graphNS;       
        this.ignite = this.getOrStart(graphNS);               
    }
}
