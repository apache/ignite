package de.kp.works.ignite;


import org.apache.ignite.binary.*;
import org.apache.ignite.configuration.IgniteConfiguration;

import java.util.Optional;

import org.apache.ignite.*;

public class IgniteConnect
{
	public static Ignite defaultIgnite = null; // first ignite
	
	private final Ignite ignite;
    private final String graphNS;
    
    
    public static IgniteConnect getInstance(IgniteConf conf) {
        return new IgniteConnect(conf);
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
    
    public Ignite getOrStart(IgniteConf cfg) {
    	Ignite ignite = null;
    	try {
    		if(defaultIgnite==null) {
    			if(Ignition.allGrids().size()==0) {
    				defaultIgnite = Ignition.start(cfg.fromFile());
    			}
    			else {
    				defaultIgnite = Ignition.allGrids().get(0);
    			}
    		}
    		ignite = Ignition.ignite(cfg.igniteName);
    		return ignite;
    	}
    	catch(Exception e) {
    		
    		try {
    			IgniteConfiguration config = cfg.fromConfig();
    			if(config==null) {
    				IgniteConf.logger.error("Can not find cfg for "+ cfg.igniteName +", use default ignite");
    				return defaultIgnite;
    			}
    			ignite = Ignition.getOrStart(config);
    			return ignite;
    		}
    		catch(Exception e2) {
    			IgniteConf.logger.error("Can not find "+ cfg.igniteName +", use default ignite", e2);
    			return defaultIgnite;
    		}
    	}
        
    }
    
    public IgniteConnect(IgniteConf conf) {
        this.graphNS = conf.igniteName;       
        this.ignite = this.getOrStart(conf);               
    }
}
